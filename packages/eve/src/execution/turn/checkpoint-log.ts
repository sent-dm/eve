import {
  createStreamStorageScope,
  type StreamStorageScope,
} from "#execution/session/stream-storage.js";
import type { SnapshotRecordRef, SnapshotStreamRef } from "#execution/session/resources.js";
import { sessionSnapshots, type StoredSnapshot } from "#execution/session/snapshots.js";
import type { AcceptedSubmission, SessionCheckpoint } from "#execution/turn/types.js";

export interface SessionBootstrap {
  readonly phase: "seed";
  readonly writeId: string;
  readonly submission: AcceptedSubmission;
}

export interface CheckpointAttempt {
  readonly phase: "entered";
  readonly writeId: string;
  readonly writerRunId: string;
  readonly source: { readonly ref: SnapshotRecordRef } | { readonly initial: SessionCheckpoint };
}

export type TurnCheckpointRecord = SessionCheckpoint | CheckpointAttempt | SessionBootstrap;

/** Effects begin after a small durable marker; only commits copy the session state. */
export async function openCheckpointLog(
  stream: SnapshotStreamRef,
  scope: StreamStorageScope = createStreamStorageScope(),
) {
  const log = await sessionSnapshots.open<TurnCheckpointRecord>(stream, scope);
  const read = async (
    ref?: SnapshotRecordRef,
  ): Promise<StoredSnapshot<SessionCheckpoint> | undefined> => {
    const stored = ref === undefined ? log.latest : { ref, checkpoint: await log.read(ref) };
    if (stored === undefined) return undefined;
    if (stored.checkpoint.phase === "seed") return undefined;
    if (stored.checkpoint.phase !== "entered")
      return { ref: stored.ref, checkpoint: stored.checkpoint };
    const source = stored.checkpoint.source;
    if ("initial" in source) return { ref: stored.ref, checkpoint: source.initial };
    const checkpoint = await log.read(source.ref);
    if (checkpoint.phase === "entered" || checkpoint.phase === "seed")
      throw new Error("A checkpoint attempt must reference committed state.");
    return { ref: source.ref, checkpoint };
  };
  return {
    get effectsOwnerRunId(): string | undefined {
      const head = log.latest?.checkpoint;
      return head?.phase === "entered" || head?.phase === "running" ? head.writerRunId : undefined;
    },
    get bootstrap(): AcceptedSubmission | undefined {
      return log.latest?.checkpoint.phase === "seed" ? log.latest.checkpoint.submission : undefined;
    },
    get hasUncommittedEffects(): boolean {
      return log.latest?.checkpoint.phase === "entered";
    },
    completed(writeId: string): StoredSnapshot<SessionCheckpoint> | undefined {
      const head = log.latest;
      return head?.checkpoint.writeId === writeId &&
        head.checkpoint.phase !== "entered" &&
        head.checkpoint.phase !== "seed"
        ? { ref: head.ref, checkpoint: head.checkpoint }
        : undefined;
    },
    entered(writeId: string): boolean {
      return log.latest?.checkpoint.writeId === `${writeId}:entered`;
    },
    assertCurrent(ref: SnapshotRecordRef): void {
      if (log.latest?.ref.streamId !== ref.streamId || log.latest.ref.index !== ref.index) {
        throw new Error("The turn checkpoint is no longer the session's current state.");
      }
    },
    read,
    begin(writeId: string, checkpoint: SessionCheckpoint, previous?: SnapshotRecordRef) {
      const head = log.latest;
      const source =
        previous === undefined
          ? { initial: checkpoint }
          : head?.checkpoint.phase === "entered" &&
              head.ref.index === previous.index &&
              head.ref.streamId === previous.streamId
            ? head.checkpoint.source
            : { ref: previous };
      return log.append({
        phase: "entered",
        writeId: `${writeId}:entered`,
        writerRunId: checkpoint.writerRunId,
        source,
      });
    },
    commit(checkpoint: SessionCheckpoint) {
      return log.append(checkpoint);
    },
  };
}
