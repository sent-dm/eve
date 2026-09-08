import type { GetWorkflowRunParams, World, WorkflowRun } from "@workflow/world";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WORKFLOW_DESERIALIZE, WORKFLOW_SERIALIZE } from "@workflow/serde";
import { getRun, Run, type WorkflowRunStream } from "#workflow-sdk/runtime/run.js";
import { getWorldLazy } from "#workflow-sdk/runtime/get-world-lazy.js";
import { bytesToBase64, deriveRunKeyPair } from "#workflow-sdk/sealed-box.js";
import {
  STREAM_NAME_SYMBOL,
  STREAM_SERVER_DEPLOYMENT_ID_SYMBOL,
  STREAM_SERVER_PUBLIC_KEY_SYMBOL,
  STREAM_SERVER_RUN_ID_SYMBOL,
} from "#workflow-sdk/symbols.js";

vi.mock("#workflow-sdk/runtime/get-world-lazy.js", () => ({ getWorldLazy: vi.fn() }));

const RUN = "wrun_reference_owner";
const DEPLOYMENT = "dpl_reference_owner";
const MATERIAL = new Uint8Array(32).fill(0x4d);

async function fixture({ encrypted = true, material = MATERIAL } = {}) {
  const pair = await deriveRunKeyPair(material);
  const record: WorkflowRun = {
    runId: RUN,
    deploymentId: DEPLOYMENT,
    status: "running",
    createdAt: new Date(),
    updatedAt: new Date(),
    workflowName: "owner-workflow",
    attributes: { tenant: "tenant-one" },
    input: { large: "must not be copied" },
    ...(encrypted ? { encryptionPublicKey: bytesToBase64(pair.publicKey) } : {}),
  };
  const chunks = new Map<string, Uint8Array[]>();
  const closed = new Set<string>();
  const world = {
    runs: {
      get: vi.fn(async (_id: string, params?: GetWorkflowRunParams) =>
        params?.resolveData === "none"
          ? { ...record, input: undefined, output: undefined }
          : record,
      ),
    },
    streams: {
      write: vi.fn(async (_runId: string, name: string, chunk: Uint8Array) => {
        const values = chunks.get(name) ?? [];
        values.push(chunk.slice());
        chunks.set(name, values);
      }),
      get: vi.fn(
        async (_runId: string, name: string, startIndex = 0) =>
          new ReadableStream<Uint8Array>({
            start(controller) {
              for (const value of chunks.get(name)?.slice(startIndex) ?? [])
                controller.enqueue(value);
              if (closed.has(name)) controller.close();
            },
          }),
      ),
      getInfo: vi.fn(async (_runId: string, name: string) => ({
        tailIndex: (chunks.get(name)?.length ?? 0) - 1,
        done: closed.has(name),
      })),
      close: vi.fn(async (_runId: string, name: string) => {
        closed.add(name);
      }),
    },
    ...(encrypted
      ? {
          getEncryptionKeyForRun: vi.fn(async (run: unknown, context?: Record<string, unknown>) => {
            if (typeof run === "string" && context?.deploymentId !== DEPLOYMENT)
              throw new Error("Wrong owner deployment");
            return material;
          }),
        }
      : {}),
  };
  vi.mocked(getWorldLazy, { partial: true, deep: true }).mockResolvedValue({
    ...world,
    runs: { get: world.runs.get as World["runs"]["get"] },
  });
  return { record, world, chunks, closed };
}

async function write(stream: WorkflowRunStream, value: unknown, close = false, namespace?: string) {
  const ops: Promise<unknown>[] = [];
  const options: WorkflowRunWritableStreamOptions = { ops };
  if (namespace !== undefined) options.namespace = namespace;
  const writable = await stream.getWritable(options);
  const writer = writable.getWriter();
  try {
    await writer.write(value);
    if (close) await writer.close();
  } finally {
    writer.releaseLock();
  }
  await Promise.all(ops);
  return { writable, ops };
}

async function read(
  stream: WorkflowRunStream,
  startIndex = 0,
  complete = false,
  namespace?: string,
) {
  const ops: Promise<unknown>[] = [];
  const reason = new Error("Reader finished");
  const options: WorkflowReadableStreamOptions = { startIndex, ops };
  if (namespace !== undefined) options.namespace = namespace;
  const reader = stream.getReadable(options).getReader();
  try {
    const value = (await reader.read()).value;
    if (complete) expect((await reader.read()).done).toBe(true);
    return value;
  } finally {
    if (!complete) await reader.cancel(reason);
    reader.releaseLock();
    for (const result of await Promise.allSettled(ops))
      if (result.status === "rejected") expect(result.reason).toBe(reason);
  }
}

afterEach(() => vi.clearAllMocks());

describe("resolved stream references", () => {
  it("issues only public immutable fields from one canonical owner lookup", async () => {
    const { world, record } = await fixture();
    const owner = getRun(RUN);
    const first = await owner.getStreamReference({ namespace: "snapshots" });
    const second = await owner.getStreamReference({ namespace: "events" });
    expect(world.runs.get).toHaveBeenCalledExactlyOnceWith(RUN, { resolveData: "none" });
    expect(world.getEncryptionKeyForRun).not.toHaveBeenCalled();
    expect(first).toEqual({
      runId: RUN,
      deploymentId: DEPLOYMENT,
      encryptionPublicKey: record.encryptionPublicKey,
      namespace: "snapshots",
    });
    expect(Object.isFrozen(first)).toBe(true);
    expect(JSON.parse(JSON.stringify(first))).toEqual(first);
    expect(second.namespace).toBe("events");
    expect(Run[WORKFLOW_SERIALIZE](owner)).toEqual({ runId: RUN, resilientStart: false });
  });

  it.each([true, false])(
    "round-trips snapshot values with all subsequent run lookups disabled (encrypted=%s)",
    async (encrypted) => {
      const { world, chunks } = await fixture({ encrypted });
      const reference = await getRun(RUN).getStreamReference({ namespace: "snapshots" });
      world.runs.get.mockClear();
      world.runs.get.mockRejectedValue(new Error("No run lookup permitted"));
      const stream = Run.fromStreamReference(structuredClone(reference));
      const value = { state: new Map([["hello", new Uint8Array([4, 3, 2, 1])]]) };
      await write(stream, value);
      expect(await read(stream)).toEqual(value);
      await write(stream, { next: true });
      expect(await read(stream, 1)).toEqual({ next: true });
      expect(world.runs.get).not.toHaveBeenCalled();
      expect(world.getEncryptionKeyForRun?.mock.calls ?? []).toEqual(
        encrypted ? [[RUN, { deploymentId: DEPLOYMENT }]] : [],
      );
      expect(world.streams.close).not.toHaveBeenCalled();
      if (encrypted)
        for (const values of chunks.values())
          for (const frame of values)
            expect(new TextDecoder().decode(frame.subarray(4, 8))).toBe("encp");
    },
  );

  it("shares concurrent read keys and keeps public write keys and durability operations separate", async () => {
    const { world, record } = await fixture();
    const ref = await getRun(RUN).getStreamReference({ namespace: "shared" });
    const writerOnly = Run.fromStreamReference(ref);
    await write(writerOnly, "first");
    expect(world.getEncryptionKeyForRun).not.toHaveBeenCalled();
    world.runs.get.mockClear();
    const stream = Run.fromStreamReference(ref);
    expect(await Promise.all([read(stream), read(stream)])).toEqual(["first", "first"]);
    const [one, two] = await Promise.all([write(stream, "second"), write(stream, "third")]);
    expect(one.ops).not.toBe(two.ops);
    expect(world.getEncryptionKeyForRun).toHaveBeenCalledExactlyOnceWith(RUN, {
      deploymentId: DEPLOYMENT,
    });
    expect(world.runs.get).not.toHaveBeenCalled();
    for (const { writable } of [one, two]) {
      expect(Reflect.get(writable, STREAM_SERVER_RUN_ID_SYMBOL)).toBe(RUN);
      expect(Reflect.get(writable, STREAM_SERVER_DEPLOYMENT_ID_SYMBOL)).toBe(DEPLOYMENT);
      expect(Reflect.get(writable, STREAM_SERVER_PUBLIC_KEY_SYMBOL)).toBe(
        record.encryptionPublicKey,
      );
    }
  });

  it("binds independent namespaces and closes only the addressed stream", async () => {
    const { world, closed } = await fixture({ encrypted: false });
    const owner = getRun(RUN);
    const one = Run.fromStreamReference(await owner.getStreamReference({ namespace: "one" }));
    const two = Run.fromStreamReference(await owner.getStreamReference({ namespace: "two" }));
    const a = await write(one, "one", true);
    const b = await write(two, "two");
    expect(closed).toEqual(new Set([Reflect.get(a.writable, STREAM_NAME_SYMBOL)]));
    expect(Reflect.get(a.writable, STREAM_NAME_SYMBOL)).not.toBe(
      Reflect.get(b.writable, STREAM_NAME_SYMBOL),
    );
    expect(await read(one, 0, true)).toBe("one");
    expect(await read(two)).toBe("two");
    expect(world.runs.get).toHaveBeenCalledTimes(1);
  });

  it("shares one resolved owner across explicit independent namespaces", async () => {
    const { world } = await fixture();
    const reference = await getRun(RUN).getStreamReference();
    expect(reference.namespace).toBeUndefined();
    world.runs.get.mockClear();
    world.runs.get.mockRejectedValue(new Error("No metadata allowed"));
    const owner = Run.fromStreamReference(reference);
    await write(owner, "events", false, "events");
    await write(owner, "snapshot", false, "snapshots");
    expect(await read(owner, 0, false, "events")).toBe("events");
    expect(await read(owner, 0, false, "snapshots")).toBe("snapshot");
    expect(world.runs.get).not.toHaveBeenCalled();
    expect(world.getEncryptionKeyForRun).toHaveBeenCalledExactlyOnceWith(RUN, {
      deploymentId: DEPLOYMENT,
    });
    const names = new Set(world.streams.write.mock.calls.map((call) => call[1]));
    expect(names.size).toBe(2);
    const defaulted = Run.fromStreamReference({ ...reference, namespace: "events" });
    expect(await read(defaulted)).toBe("events");
    expect(await read(defaulted, 0, false, "snapshots")).toBe("snapshot");
  });

  it("cancels an unconsumed reference reader without opening transport or resolving keys", async () => {
    const { world } = await fixture();
    const ref = await getRun(RUN).getStreamReference();
    world.runs.get.mockClear();
    const ops: Promise<unknown>[] = [];
    const readable = Run.fromStreamReference(ref).getReadable({ ops });
    await readable.cancel(new Error("No longer needed"));
    expect(world.runs.get).not.toHaveBeenCalled();
    expect(world.streams.get).not.toHaveBeenCalled();
    expect(world.getEncryptionKeyForRun).not.toHaveBeenCalled();
    expect(ops).toHaveLength(0);
  });

  it("fails a denied key lookup and retries it without a metadata or current-deployment fallback", async () => {
    const { world } = await fixture();
    const ref = await getRun(RUN).getStreamReference();
    await write(Run.fromStreamReference(ref), "private");
    world.runs.get.mockClear();
    world.getEncryptionKeyForRun!.mockRejectedValueOnce(new Error("Read access denied"));
    const stream = Run.fromStreamReference(ref);
    await expect(read(stream)).rejects.toThrow("Read access denied");
    expect(await read(stream)).toBe("private");
    expect(world.getEncryptionKeyForRun!.mock.calls).toEqual([
      [RUN, { deploymentId: DEPLOYMENT }],
      [RUN, { deploymentId: DEPLOYMENT }],
    ]);
    expect(world.runs.get).not.toHaveBeenCalled();
  });

  it("does not change the full-run key callback of ordinary Run access", async () => {
    const { world, record } = await fixture();
    const run = getRun(RUN);
    const ref = await run.getStreamReference();
    await write(Run.fromStreamReference(ref), "value");
    expect(
      await read({
        getReadable: (options) => run.getReadable(options),
        getWritable: (options) => run.getWritable(options),
      }),
    ).toBe("value");
    expect(world.getEncryptionKeyForRun).toHaveBeenCalledExactlyOnceWith(record);
    expect(world.runs.get).toHaveBeenCalledTimes(2);
    const serialized = Run[WORKFLOW_SERIALIZE](run);
    const revived = Run[WORKFLOW_DESERIALIZE](serialized);
    await revived.getStreamReference();
    expect(world.runs.get).toHaveBeenCalledTimes(3);
  });

  it("does not carry keys between reconstructed stream handles or World instances", async () => {
    const a = await fixture();
    const ref = await getRun(RUN).getStreamReference();
    const one = Run.fromStreamReference(ref);
    await write(one, "a");
    expect(await read(one)).toBe("a");
    const b = await fixture();
    const two = Run.fromStreamReference(structuredClone(ref));
    await write(two, "b");
    expect(await read(two)).toBe("b");
    expect(a.world.getEncryptionKeyForRun).toHaveBeenCalledTimes(1);
    expect(b.world.getEncryptionKeyForRun).toHaveBeenCalledTimes(1);
    expect(b.world.runs.get).not.toHaveBeenCalled();
  });

  it("rejects missing deployment routing and malformed public keys before opening transport", async () => {
    const { world } = await fixture();
    const ref = await getRun(RUN).getStreamReference();
    world.runs.get.mockClear();
    expect(() => Run.fromStreamReference({ ...ref, deploymentId: "" })).toThrow("deployment IDs");
    expect(() =>
      Run.fromStreamReference({ ...ref, encryptionPublicKey: "not-a-public-key" }),
    ).toThrow("invalid owner public key");
    expect(world.runs.get).not.toHaveBeenCalled();
    expect(world.getEncryptionKeyForRun).not.toHaveBeenCalled();
    expect(world.streams.get).not.toHaveBeenCalled();
  });
});

it("a public reference can write sealed frames without granting read access", async () => {
  const { world, chunks } = await fixture();
  const ref = await getRun(RUN).getStreamReference();
  world.runs.get.mockClear();
  const { getEncryptionKeyForRun: _readKey, ...writeOnlyWorld } = world;
  vi.mocked(getWorldLazy, { partial: true, deep: true }).mockResolvedValue({
    ...writeOnlyWorld,
    runs: { get: world.runs.get as World["runs"]["get"] },
  });
  const stream = Run.fromStreamReference(ref);
  await write(stream, "sealed-only");
  for (const values of chunks.values())
    for (const frame of values) expect(new TextDecoder().decode(frame.subarray(4, 8))).toBe("encp");
  await expect(read(stream)).rejects.toThrow("no run keypair is available");
  expect(world.runs.get).not.toHaveBeenCalled();
});
