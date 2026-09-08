import { getRun, Run } from "#internal/workflow/runtime.js";
import { decodeStreamLocation, type StreamOwner } from "#execution/session/stream-location.js";

const READ_TIMEOUT_MS = 10_000;

type StreamAccess = Pick<ReturnType<typeof getRun>, "getReadable" | "getWritable">;

/** Resolve public owner routing once while initializing the holder. */
export async function resolveStreamOwner(runId: string): Promise<StreamOwner> {
  const reference = await getRun(runId).getStreamReference();
  return {
    runId: reference.runId,
    deploymentId: reference.deploymentId,
    ...(reference.encryptionPublicKey === undefined
      ? {}
      : { encryptionPublicKey: reference.encryptionPublicKey }),
  };
}

export type StreamStorage = ReturnType<typeof storageForOwner>;

export interface StreamStorageScope {
  open(id: string): StreamStorage;
}

/** Owner/key resolution lives only as long as the current step's scope. */
export function createStreamStorageScope(): StreamStorageScope {
  const owners = new Map<string, StreamAccess>();
  return {
    open(id) {
      const { owner, namespace } = decodeStreamLocation(id);
      const key = typeof owner === "string" ? owner : owner.runId;
      let streams = owners.get(key);
      if (streams === undefined) {
        streams = typeof owner === "string" ? getRun(owner) : Run.fromStreamReference(owner);
        owners.set(key, streams);
      }
      return storageForOwner(streams, namespace);
    },
  };
}

export function openStreamStorage(id: string): StreamStorage {
  return createStreamStorageScope().open(id);
}

function storageForOwner(owner: StreamAccess, namespace?: string) {
  const read = <T>(startIndex?: number) => owner.getReadable<T>({ namespace, startIndex });
  const withWriter = async <T, Result>(
    run: (writable: WritableStream<T>) => Promise<Result>,
  ): Promise<Result> => {
    const ops: Promise<unknown>[] = [];
    const writable = await owner.getWritable<T>({ namespace, ops });
    return await contribute(writable, ops, run);
  };
  return {
    read,
    async tailIndex(): Promise<number> {
      const readable = read();
      try {
        return await readable.getTailIndex();
      } finally {
        await readable.cancel();
      }
    },
    readRecord<T>(startIndex = 0): Promise<T> {
      return readRecord(read<T>(startIndex));
    },
    withWriter,
    async append<T>(records: readonly T[], close = false): Promise<void> {
      await withWriter<T, void>(async (writable) => {
        const writer = writable.getWriter();
        try {
          for (const record of records) await writer.write(record);
          if (close) await writer.close();
        } finally {
          writer.releaseLock();
        }
      });
    },
  };
}

/** Reads one existing record, or waits once for holder initialization. */
export function readStreamRecord<T>(id: string, startIndex = 0): Promise<T> {
  return openStreamStorage(id).readRecord<T>(startIndex);
}

async function readRecord<T>(readable: ReadableStream<T>): Promise<T> {
  const reader = readable.getReader();
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      reader.read(),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error("Session storage read timed out.")),
          READ_TIMEOUT_MS,
        );
      }),
    ]);
    if (result.done) throw new Error("Session storage record does not exist.");
    return result.value;
  } finally {
    clearTimeout(timeout);
    await reader.cancel();
    reader.releaseLock();
  }
}

async function contribute<T, Result>(
  stream: WritableStream<T>,
  ops: readonly Promise<unknown>[],
  run: (writable: WritableStream<T>) => Promise<Result>,
): Promise<Result> {
  const owner = stream.getWriter();
  // The SDK treats an unlocked writer as finished. Borrower lock gaps must not
  // settle its durability barrier before the complete callback has written.
  const writable = new WritableStream<T>({
    write: (value) => owner.write(value),
    close: () => owner.close(),
    abort: (reason) => owner.abort(reason),
  });
  let outcome: { kind: "returned"; value: Result } | { kind: "threw"; error: unknown };
  try {
    outcome = { kind: "returned", value: await run(writable) };
  } catch (error) {
    outcome = { kind: "threw", error };
  } finally {
    owner.releaseLock();
  }
  const failures: unknown[] = [];
  if (writable.locked) {
    failures.push(
      new Error("Session stream writer must be released before completing its operation."),
    );
  }
  failures.push(
    ...(await Promise.allSettled(ops)).flatMap((result) =>
      result.status === "rejected" ? [result.reason] : [],
    ),
  );
  if (outcome.kind === "threw") {
    if (failures.length > 0)
      throw new AggregateError(
        [outcome.error, ...failures],
        "Session stream operation and durability flush failed.",
      );
    throw outcome.error;
  }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1)
    throw new AggregateError(failures, "Session stream durability flush failed.");
  return outcome.value;
}
