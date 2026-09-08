import { beforeEach, describe, expect, it, vi } from "vitest";
import { createSessionResources, type SnapshotRecordRef } from "#execution/session/resources.js";
import type { SnapshotLog } from "#execution/session/snapshots.js";
import { createDurableSessionState } from "#execution/session/state.js";
import {
  openCheckpointLog,
  type CheckpointAttempt,
  type TurnCheckpointRecord,
} from "#execution/turn/checkpoint-log.js";
import type { InitializedSessionCheckpoint } from "#execution/turn/types.js";

const mocks = vi.hoisted(() => ({ open: vi.fn(), read: vi.fn(), append: vi.fn() }));
vi.mock("#execution/session/snapshots.js", () => ({ sessionSnapshots: { open: mocks.open } }));

const session = createSessionResources("holder", "initial", {
  runId: "holder",
  deploymentId: "owner-deployment",
});
const records: TurnCheckpointRecord[] = [];
const ref = (index: number): SnapshotRecordRef => ({ streamId: session.snapshots.id, index });
const committed: InitializedSessionCheckpoint = {
  phase: "settled",
  writeId: "previous",
  writerRunId: "previous-owner",
  deliveries: {},
  queue: [],
  serializedContext: { retained: "context" },
  state: createDurableSessionState({
    session: {
      sessionId: session.sessionId,
      continuationToken: "alias",
      history: [{ role: "user", content: "History must remain behind its reference." }],
      agent: { modelReference: { id: "model" }, system: "", tools: [] },
      compaction: { threshold: 1000, recentWindowSize: 10 },
    },
  }),
};
const pending: InitializedSessionCheckpoint = {
  ...committed,
  phase: "running",
  writerRunId: "owner",
};

beforeEach(() => {
  vi.resetAllMocks();
  records.length = 0;
  mocks.read.mockImplementation(async (target: SnapshotRecordRef) => {
    const record = records[target.index - 1];
    if (target.streamId !== session.snapshots.id || record === undefined)
      throw new Error("Unknown snapshot reference.");
    return record;
  });
  mocks.append.mockImplementation(async (record: TurnCheckpointRecord) => {
    records.push(record);
    return ref(records.length);
  });
  mocks.open.mockImplementation(
    async () =>
      ({
        get latest() {
          const checkpoint = records.at(-1);
          return checkpoint === undefined ? undefined : { ref: ref(records.length), checkpoint };
        },
        read: mocks.read,
        append: mocks.append,
      }) satisfies SnapshotLog<TurnCheckpointRecord>,
  );
});

describe("turn checkpoint log", () => {
  it("exposes bootstrap input without treating it as initialized session state", async () => {
    const submission = { eventId: "first", command: { kind: "cancel" as const } };
    records.push({ phase: "seed", writeId: "first", submission });
    const log = await openCheckpointLog(session.snapshots);
    expect(log.bootstrap).toEqual(submission);
    expect(await log.read()).toBeUndefined();
    expect(log.completed("first")).toBeUndefined();
    expect(log.hasUncommittedEffects).toBe(false);
    await log.begin("initialize", pending);
    expect(log.bootstrap).toBeUndefined();
    expect(records[1]).toMatchObject({ source: { initial: pending } });
  });
  it("represents empty bootstrap state without searching for a checkpoint", async () => {
    const log = await openCheckpointLog(session.snapshots);
    expect(await log.read()).toBeUndefined();
    expect(log.completed("model")).toBeUndefined();
    expect(log.entered("model")).toBe(false);
    expect(log.hasUncommittedEffects).toBe(false);
    expect(mocks.read).not.toHaveBeenCalled();
    expect(mocks.append).not.toHaveBeenCalled();
  });

  it("keeps warm attempt markers small and recovers only their exact prior checkpoint", async () => {
    records.push(committed);
    const log = await openCheckpointLog(session.snapshots);
    const attempt = await log.begin("model", pending, ref(1));
    expect(records[1]).toEqual({
      phase: "entered",
      writeId: "model:entered",
      writerRunId: "owner",
      source: { ref: ref(1) },
    });
    expect(JSON.stringify(records[1])).not.toContain("history");
    expect(JSON.stringify(records[1])).not.toContain("serializedContext");
    expect(attempt).toEqual(ref(2));
    expect(log.hasUncommittedEffects).toBe(true);
    expect(log.entered("model")).toBe(true);
    expect(log.completed("model:entered")).toBeUndefined();
    expect(await log.read()).toEqual({ ref: ref(1), checkpoint: committed });
    expect(mocks.read).toHaveBeenCalledExactlyOnceWith(ref(1));
  });

  it("embeds cold initialization once and preserves that source during failure recovery", async () => {
    const log = await openCheckpointLog(session.snapshots);
    const entered = await log.begin("initialize", pending);
    expect(records[0]).toMatchObject({ source: { initial: pending } });
    const recovery = await openCheckpointLog(session.snapshots);
    expect(await recovery.read()).toEqual({ ref: entered, checkpoint: pending });
    await recovery.begin("failure", pending, entered);
    expect(records[1]).toEqual({
      phase: "entered",
      writeId: "failure:entered",
      writerRunId: "owner",
      source: { initial: pending },
    });
    expect(mocks.read).not.toHaveBeenCalled();
  });

  it("retains the committed source when failure recovery replaces a warm attempt", async () => {
    records.push(committed);
    const log = await openCheckpointLog(session.snapshots);
    const entered = await log.begin("model", pending, ref(1));
    await log.begin("failure", pending, entered);
    expect(records[2]).toMatchObject({ source: { ref: ref(1) } });
    expect(await log.read()).toEqual({ ref: ref(1), checkpoint: committed });
    expect(mocks.read).toHaveBeenCalledExactlyOnceWith(ref(1));
  });

  it("recognizes only the committed head on retry without scanning earlier operation IDs", async () => {
    records.push(committed);
    const log = await openCheckpointLog(session.snapshots);
    await log.begin("model", pending, ref(1));
    const result = { ...pending, writeId: "model" };
    const resultRef = await log.commit(result);
    const retry = await openCheckpointLog(session.snapshots);
    expect(retry.completed("model")).toEqual({ ref: resultRef, checkpoint: result });
    expect(retry.completed("previous")).toBeUndefined();
    expect(retry.entered("model")).toBe(false);
    expect(retry.hasUncommittedEffects).toBe(false);
    expect(await retry.read()).toEqual({ ref: resultRef, checkpoint: result });
    expect(mocks.read).not.toHaveBeenCalled();
  });

  it("rejects obsolete or foreign input references before any historical read", async () => {
    records.push(committed, { ...pending, writeId: "model" });
    const log = await openCheckpointLog(session.snapshots);
    expect(() => log.assertCurrent(ref(2))).not.toThrow();
    expect(() => log.assertCurrent(ref(1))).toThrow("no longer");
    expect(() =>
      log.assertCurrent({
        streamId: createSessionResources("other", "initial", {
          runId: "other",
          deploymentId: "owner-deployment",
        }).snapshots.id,
        index: 2,
      }),
    ).toThrow("no longer");
    expect(mocks.read).not.toHaveBeenCalled();
  });

  it("rejects a marker chain instead of following an unbounded recovery history", async () => {
    const first: CheckpointAttempt = {
      phase: "entered",
      writeId: "first:entered",
      writerRunId: "owner",
      source: { initial: pending },
    };
    records.push(first, { ...first, writeId: "second:entered", source: { ref: ref(1) } });
    const log = await openCheckpointLog(session.snapshots);
    await expect(log.read()).rejects.toThrow("must reference committed state");
    expect(mocks.read).toHaveBeenCalledExactlyOnceWith(ref(1));
  });
});
