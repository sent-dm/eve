import { equalSnapshot } from "#execution/session/snapshot-equality.js";
import type { SnapshotRecordRef, SnapshotStreamRef } from "#execution/session/resources.js";
import { openStreamStorage } from "#execution/session/stream-storage.js";

interface SnapshotWrite {
  readonly writeId: string;
}

type SnapshotEntry<Checkpoint> =
  | { readonly kind: "initialized" }
  | { readonly kind: "record"; readonly checkpoint: Checkpoint };

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
  if (!Number.isSafeInteger(ref.index) || ref.index < 1) {
    throw new Error("Invalid session snapshot record index.");
  }
}

function checkpointFromEntry<Checkpoint>(entry: SnapshotEntry<Checkpoint>): Checkpoint {
  if (entry.kind !== "record") throw new Error("Session snapshot record does not exist.");
  return entry.checkpoint;
}

export const sessionSnapshots = {
  async initialize(ref: SnapshotStreamRef): Promise<void> {
    const storage = openStreamStorage(ref.id);
    if ((await storage.tailIndex()) === -1) {
      await storage.append<SnapshotEntry<never>>([{ kind: "initialized" }]);
    }
  },

  async open<Checkpoint extends SnapshotWrite>(
    stream: SnapshotStreamRef,
  ): Promise<SnapshotLog<Checkpoint>> {
    const storage = openStreamStorage(stream.id);
    let index = await storage.tailIndex();
    if (index === -1) throw new Error("Session snapshot storage has not been initialized.");
    const head = await storage.readRecord<SnapshotEntry<Checkpoint>>(index);
    if (head.kind === "initialized" && index !== 0) {
      throw new Error("Session snapshot storage has an invalid initialization record.");
    }
    let latest: StoredSnapshot<Checkpoint> | undefined =
      head.kind === "record"
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
          : checkpointFromEntry(await storage.readRecord<SnapshotEntry<Checkpoint>>(ref.index));
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
            );
            if (!(await equalSnapshot(stored, checkpoint))) {
              throw new Error(
                `Snapshot write identity "${checkpoint.writeId}" was reused with different state.`,
              );
            }
            return latest.ref;
          }
          const ref: SnapshotRecordRef = { streamId: stream.id, index: index + 1 };
          try {
            // The SDK stores each serialized object as one indexed stream chunk.
            await storage.append<SnapshotEntry<Checkpoint>>([{ kind: "record", checkpoint }]);
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

  async latest<Checkpoint extends SnapshotWrite>(
    stream: SnapshotStreamRef,
  ): Promise<StoredSnapshot<Checkpoint> | undefined> {
    return (await sessionSnapshots.open<Checkpoint>(stream)).latest;
  },

  async read<Checkpoint>(ref: SnapshotRecordRef): Promise<Checkpoint> {
    validateRecordRef(ref);
    return checkpointFromEntry(
      await openStreamStorage(ref.streamId).readRecord<SnapshotEntry<Checkpoint>>(ref.index),
    );
  },

  close(ref: SnapshotStreamRef): Promise<void> {
    return openStreamStorage(ref.id).append([], true);
  },
};
