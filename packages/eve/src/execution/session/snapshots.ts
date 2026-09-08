import { equalSnapshot } from "#execution/session/snapshot-equality.js";
import type { SnapshotRecordRef, SnapshotStreamRef } from "#execution/session/resources.js";
import {
  createStreamStorageScope,
  type StreamStorageScope,
} from "#execution/session/stream-storage.js";

interface SnapshotWrite {
  readonly writeId: string;
}

type SnapshotEntry<Checkpoint> =
  | { readonly kind: "initialized"; readonly index: 0; readonly checkpoint?: Checkpoint }
  | { readonly kind: "record"; readonly index: number; readonly checkpoint: Checkpoint };

export interface StoredSnapshot<Checkpoint> {
  readonly ref: SnapshotRecordRef;
  readonly checkpoint: Checkpoint;
}

/** One step holds the session's exclusive ownership while using this log. */
export interface SnapshotLog<Checkpoint> {
  readonly latest: StoredSnapshot<Checkpoint> | undefined;
  read(ref: SnapshotRecordRef): Promise<Checkpoint>;
  append(checkpoint: Checkpoint): Promise<SnapshotRecordRef>;
}

function validateRecordRef(ref: SnapshotRecordRef): void {
  if (!Number.isSafeInteger(ref.index) || ref.index < 0) {
    throw new Error("Invalid session snapshot record index.");
  }
}

function validateEntry<Checkpoint>(entry: SnapshotEntry<Checkpoint>): void {
  if (
    !Number.isSafeInteger(entry.index) ||
    entry.index < 0 ||
    (entry.kind === "initialized"
      ? entry.index !== 0
      : entry.kind !== "record" || entry.index === 0)
  ) {
    throw new Error("Session snapshot storage has an invalid record index.");
  }
}

function checkpointFromEntry<Checkpoint>(
  entry: SnapshotEntry<Checkpoint>,
  index: number,
): Checkpoint {
  validateEntry(entry);
  if (entry.checkpoint === undefined) throw new Error("Session snapshot record does not exist.");
  if (entry.index !== index)
    throw new Error("Session snapshot record index does not match its reference.");
  return entry.checkpoint;
}

export const sessionSnapshots = {
  async initialize<Checkpoint extends SnapshotWrite>(
    ref: SnapshotStreamRef,
    scope: StreamStorageScope = createStreamStorageScope(),
    checkpoint?: Checkpoint,
  ): Promise<void> {
    const storage = scope.open(ref.id);
    if ((await storage.tailIndex()) === -1) {
      await storage.append<SnapshotEntry<Checkpoint>>([
        { kind: "initialized", index: 0, checkpoint },
      ]);
    }
  },

  async open<Checkpoint extends SnapshotWrite>(
    stream: SnapshotStreamRef,
    scope: StreamStorageScope = createStreamStorageScope(),
  ): Promise<SnapshotLog<Checkpoint>> {
    const storage = scope.open(stream.id);
    const head = await storage.readRecord<SnapshotEntry<Checkpoint>>(-1);
    validateEntry(head);
    let index = head.index;
    let latest: StoredSnapshot<Checkpoint> | undefined =
      head.checkpoint !== undefined
        ? { ref: { streamId: stream.id, index }, checkpoint: head.checkpoint }
        : undefined;
    let latestWriteId = latest?.checkpoint.writeId;
    let failed = false;
    let writing = false;
    function assertAvailable(): void {
      if (failed)
        throw new Error("Session snapshot write failed; reopen the log before continuing.");
    }
    return {
      get latest() {
        assertAvailable();
        return latest;
      },
      async read(ref) {
        assertAvailable();
        validateRecordRef(ref);
        if (ref.streamId !== stream.id || ref.index > index) {
          throw new Error("Session snapshot record is outside this log.");
        }
        return latest?.ref.index === ref.index
          ? latest.checkpoint
          : checkpointFromEntry(
              await storage.readRecord<SnapshotEntry<Checkpoint>>(ref.index),
              ref.index,
            );
      },
      async append(checkpoint) {
        assertAvailable();
        if (checkpoint.writeId.length === 0) {
          throw new Error("Snapshot writes require a stable write identity.");
        }
        if (writing) throw new Error("Session snapshot writes must be sequential.");
        writing = true;
        try {
          if (latest !== undefined && latestWriteId === checkpoint.writeId) {
            // Read the durable value on a retry; the caller may have mutated its
            // in-memory checkpoint since the original append.
            const stored = checkpointFromEntry(
              await storage.readRecord<SnapshotEntry<Checkpoint>>(latest.ref.index),
              latest.ref.index,
            );
            if (!(await equalSnapshot(stored, checkpoint))) {
              throw new Error(
                `Snapshot write identity "${checkpoint.writeId}" was reused with different state.`,
              );
            }
            return latest.ref;
          }
          const ref: SnapshotRecordRef = { streamId: stream.id, index: index + 1 };
          validateRecordRef(ref);
          try {
            // The SDK stores each serialized object as one indexed stream chunk.
            await storage.append<SnapshotEntry<Checkpoint>>([
              { kind: "record", index: ref.index, checkpoint },
            ]);
          } catch (error) {
            // A failed flush may already have committed. Only a fresh tail read
            // can establish which position the next writer owns.
            failed = true;
            throw error;
          }
          index = ref.index;
          latestWriteId = checkpoint.writeId;
          latest = { ref, checkpoint };
          return ref;
        } finally {
          writing = false;
        }
      },
    };
  },

  close(
    ref: SnapshotStreamRef,
    scope: StreamStorageScope = createStreamStorageScope(),
  ): Promise<void> {
    return scope.open(ref.id).append([], true);
  },
};
