import type { SnapshotLog } from "#execution/session/snapshots.js";
import type { TurnCheckpointRecord } from "#execution/turn/checkpoint-log.js";
import { accountPending } from "#execution/turn/submissions.js";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { failTurnStep, finalizeTurnStep } from "#execution/turn/finalize.js";
import { createSessionResources, type SnapshotRecordRef } from "#execution/session/resources.js";
import type { InitializedSessionCheckpoint } from "#execution/turn/types.js";
import { createDurableSessionState } from "#execution/session/state.js";
import {
  createSessionWaitingEvent,
  createTurnCompletedEvent,
  stampMessageStreamEvent,
} from "#protocol/message.js";

const mocks = vi.hoisted(() => ({
  stepId: "commit",
  open: vi.fn(),
  read: vi.fn(),
  append: vi.fn(),
  closeSnapshots: vi.fn(),
  appendEvents: vi.fn(),
  closeEvents: vi.fn(),
  finalize: vi.fn(),
  cancel: vi.fn(),
  cancelDescendants: vi.fn(),
  terminateChildren: vi.fn(),
  cancelTimer: vi.fn(),
  notifyCancel: vi.fn(),
  notifyCaller: vi.fn(),
  notifyParent: vi.fn(),
  callback: vi.fn(),
  cancelRun: vi.fn(),
  log: vi.fn(),
  resolveSession: vi.fn(),
}));
vi.mock("#compiled/@workflow/core/index.js", () => ({
  getStepMetadata: () => ({ stepId: mocks.stepId }),
  getWorkflowMetadata: () => ({ workflowRunId: "owner" }),
}));
vi.mock("#internal/logging.js", () => ({ createLogger: () => ({ error: mocks.log }) }));
vi.mock("#execution/turn/initialization-failure.js", () => ({
  notifyInitializationFailure: vi.fn(),
}));
vi.mock("#execution/session/snapshots.js", () => ({
  sessionSnapshots: {
    open: mocks.open,
    close: mocks.closeSnapshots,
  },
}));
vi.mock("#execution/session/events.js", () => ({
  sessionEvents: {
    open: (ref: unknown) => ({
      append: (events: unknown) => mocks.appendEvents(ref, events),
      close: () => mocks.closeEvents(ref),
      withWriter: async (callback: (writable: WritableStream<Uint8Array>) => Promise<unknown>) =>
        callback(new WritableStream()),
    }),
  },
}));
vi.mock("#execution/session/directory.js", () => ({
  resolveSessionTarget: mocks.resolveSession,
}));
vi.mock("#execution/turn/finalize-model.js", () => ({ finalizeModelSettlement: mocks.finalize }));
vi.mock("#execution/turn/cancel.js", () => ({
  cancellationSettlement: (state: InitializedSessionCheckpoint["state"], kind: string) => ({
    events: [{ type: kind === "interrupt" ? "turn.interrupted" : "turn.cancelled" }],
    emissionAfter: state.emissionState,
  }),
  settleCancelledTurn: mocks.cancel,
}));
vi.mock("#execution/turn/cancel-descendants.js", () => ({
  cancelDescendantTurns: mocks.cancelDescendants,
}));
vi.mock("#execution/turn/terminate-children.js", () => ({
  terminateChildSessions: mocks.terminateChildren,
}));
vi.mock("#execution/session-timeout-steps.js", () => ({
  cancelSessionTimeout: mocks.cancelTimer,
}));
vi.mock("#subagents/parent-notification.js", () => ({
  notifyCancelledTaskCaller: mocks.notifyCancel,
  notifyTurnCaller: mocks.notifyCaller,
  notifyDelegatedParent: mocks.notifyParent,
}));
vi.mock("#subagents/parent-result.js", () => ({
  createDelegatedSubagentErrorResult: vi.fn(),
  createDelegatedSubagentSuccessResult: vi.fn(),
}));
vi.mock("#subagents/callbacks.js", () => ({ fireSessionCallback: mocks.callback }));
vi.mock("#internal/workflow/runtime.js", () => ({
  cancelRun: mocks.cancelRun,
  getWorld: async () => ({}),
}));
vi.mock("#execution/tasks/workflow-target.js", () => ({ isTaskWorkflowTargetGone: () => false }));

const resources = createSessionResources("holder", "initial");
const records = new Map<string, TurnCheckpointRecord>();
const indices = new Map<string, number>();
let current: TurnCheckpointRecord | undefined;
function recordRef(id: string): SnapshotRecordRef {
  if (!indices.has(id)) indices.set(id, indices.size + 1);
  return { streamId: resources.snapshots.id, index: indices.get(id)! };
}

function checkpoint(): InitializedSessionCheckpoint {
  const state = createDurableSessionState({
    session: {
      sessionId: resources.sessionId,
      continuationToken: "alias",
      history: [],
      agent: { modelReference: { id: "model" }, system: "", tools: [] },
      compaction: { threshold: 1000, recentWindowSize: 10 },
    },
  });
  return {
    writeId: "proposal",
    writerRunId: "owner",
    phase: "running",
    state,
    serializedContext: { "eve.mode": "conversation" },
    deliveries: { initial: "applied" },
    queue: [],
    caller: {
      callId: "call",
      subagentName: "child",
      replyTo: { kind: "session", token: "parent" },
    },
    result: {
      action: "park",
      hasPendingAuthorization: false,
      hasPendingInputBatch: true,
      sessionState: state,
      serializedContext: {},
      settlement: {
        events: [stampMessageStreamEvent(createSessionWaitingEvent())],
        emissionAfter: state.emissionState,
      },
    },
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mocks.resolveSession.mockResolvedValue(resources);
  mocks.stepId = "commit";
  records.clear();
  current = checkpoint();
  records.set(current.writeId, current);
  indices.clear();
  mocks.open.mockImplementation(async () => {
    let latest: SnapshotLog<TurnCheckpointRecord>["latest"] =
      current === undefined
        ? undefined
        : { ref: recordRef(current.writeId), checkpoint: records.get(current.writeId)! };
    mocks.read.mockImplementation(async (target: SnapshotRecordRef) => {
      const id = [...indices].find(([, index]) => index === target.index)?.[0];
      const value = id === undefined ? undefined : records.get(id);
      if (target.streamId !== resources.snapshots.id || value === undefined)
        throw new Error("Unknown snapshot reference.");
      return value;
    });
    mocks.append.mockImplementation(async (value: TurnCheckpointRecord) => {
      records.set(value.writeId, value);
      current = value;
      const ref = recordRef(value.writeId);
      latest = { ref, checkpoint: value };
      return ref;
    });
    return {
      get latest() {
        return latest;
      },
      read: mocks.read,
      append: mocks.append,
    } satisfies SnapshotLog<TurnCheckpointRecord>;
  });
  mocks.finalize.mockImplementation(async (input) => ({
    sessionState: input.sessionState,
    serializedContext: input.serializedContext,
  }));
  mocks.cancel.mockImplementation(async (input) => ({
    sessionState: input.sessionState,
    serializedContext: input.serializedContext,
  }));
  mocks.appendEvents.mockResolvedValue(undefined);
});

describe("turn finalization", () => {
  it("expires after preserving the active turn's successful outcome and model state", async () => {
    const original = current as InitializedSessionCheckpoint;
    const proposed = original.result!;
    current = {
      ...original,
      result: {
        ...proposed,
        action: "park",
        hasPendingAuthorization: false,
        hasPendingInputBatch: false,
        settled: { output: "Completed before expiry" },
        cancellationState: { ...original.state, continuationToken: "discarded-rollback" },
        settlement: {
          events: [
            stampMessageStreamEvent(createTurnCompletedEvent({ turnId: "active", sequence: 0 })),
            stampMessageStreamEvent(createSessionWaitingEvent()),
          ],
          emissionAfter: original.state.emissionState,
        },
      },
    };
    records.set("proposal", current);
    const result = await finalizeTurnStep({
      session: resources,
      eventIds: ["initial", "expiry"],
      checkpoint: recordRef("proposal"),
      kind: "timeout",
      pending: [
        {
          kind: "session.submit",
          eventId: "expiry",
          payload: {
            candidateRunId: "timer",
            submission: { eventId: "expiry", command: { kind: "session-timeout" } },
          },
        },
      ],
    });
    expect(result).toMatchObject({ terminal: true, deliveries: { expiry: "applied" } });
    expect(mocks.cancel).not.toHaveBeenCalled();
    expect(mocks.cancelDescendants).not.toHaveBeenCalled();
    expect(mocks.notifyCancel).not.toHaveBeenCalled();
    expect(mocks.finalize.mock.lastCall?.[0].sessionState).toEqual(original.state);
    expect(
      mocks.finalize.mock.lastCall?.[0].settlement.events.map(
        (event: { type: string }) => event.type,
      ),
    ).toEqual(["turn.completed", "session.completed"]);
    expect(mocks.notifyCaller).toHaveBeenCalledWith(
      expect.objectContaining({
        lifecycle: "terminal",
        settled: { output: "Completed before expiry" },
      }),
    );
    expect(mocks.closeEvents).toHaveBeenCalledOnce();
  });
  it("keeps a caller parked on HITL and preserves the accepting candidate of queued input", async () => {
    const original = current as InitializedSessionCheckpoint;
    records.set("proposal", {
      ...original,
      deliveries: { ...original.deliveries, "older-turn": "applied" },
    });
    const pending = {
      submission: {
        eventId: "followup",
        acceptedDeploymentId: "new-deployment",
        command: { kind: "send" as const, payload: { message: "Next" } },
      },
      candidateRunId: "waiting-candidate",
    };
    const result = await finalizeTurnStep({
      session: resources,
      eventIds: ["initial", "followup"],
      checkpoint: recordRef("proposal"),
      kind: "natural",
      pending: [{ kind: "session.submit", eventId: "delivery", payload: pending }],
    });
    expect(result.terminal).toBe(false);
    expect(result.deliveries).not.toHaveProperty("older-turn");
    const committed = records.get("commit") as InitializedSessionCheckpoint;
    expect(committed.caller).toEqual(original.caller);
    expect(committed.queue).toEqual([pending]);
    expect(committed.deliveries["older-turn"]).toBe("applied");
    expect(committed.claimedContinuationToken).toBeUndefined();
    expect(mocks.notifyCaller).not.toHaveBeenCalled();
    expect(mocks.append.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.finalize.mock.invocationCallOrder[0]!,
    );
  });

  it("returns the continuation token committed by terminal lifecycle hooks before ownership releases", async () => {
    mocks.finalize.mockImplementationOnce(async (input) => ({
      serializedContext: input.serializedContext,
      sessionState: {
        ...input.sessionState,
        continuationToken: "new-alias",
        snapshot: {
          session: { ...input.sessionState.snapshot.session, continuationToken: "new-alias" },
        },
      },
    }));
    const result = await finalizeTurnStep({
      session: resources,
      eventIds: ["initial", "followup"],
      claimedContinuationToken: "acknowledged-alias",
      checkpoint: recordRef("proposal"),
      kind: "natural",
      pending: [],
    });
    expect(result).toMatchObject({ terminal: false, continuationToken: "new-alias" });
    expect((records.get("commit") as InitializedSessionCheckpoint).claimedContinuationToken).toBe(
      "acknowledged-alias",
    );
  });

  it("preserves a previously acknowledged alias when no new acknowledgment is supplied", async () => {
    current = {
      ...current,
      claimedContinuationToken: "known-alias",
    } as InitializedSessionCheckpoint;
    records.set("proposal", current);
    await finalizeTurnStep({
      session: resources,
      eventIds: ["initial", "followup"],
      checkpoint: recordRef("proposal"),
      kind: "natural",
      pending: [],
    });
    expect((records.get("commit") as InitializedSessionCheckpoint).claimedContinuationToken).toBe(
      "known-alias",
    );
  });

  it("publishes a completed outcome once and skips authored effects on a committed retry", async () => {
    const original = current as InitializedSessionCheckpoint;
    current = {
      ...original,
      result: {
        action: "done",
        output: "Answer",
        sessionState: original.state,
        serializedContext: {},
        settlement: original.result?.settlement,
      },
    };
    records.set("proposal", current);
    const input = {
      session: resources,
      eventIds: ["initial", "followup"],
      checkpoint: recordRef("proposal"),
      kind: "natural" as const,
      pending: [],
    };
    const first = await finalizeTurnStep(input);
    expect(first.terminal).toBe(true);
    expect(mocks.notifyCaller).toHaveBeenCalledTimes(1);
    expect(await finalizeTurnStep(input)).toEqual(first);
    expect(mocks.finalize).toHaveBeenCalledTimes(1);
    expect(mocks.notifyCaller).toHaveBeenCalledTimes(1);
    expect((records.get("commit") as InitializedSessionCheckpoint).caller).toBeUndefined();
  });

  it("uses the cancellation carveouts when an interrupt overtakes a completed model call", async () => {
    const original = current as InitializedSessionCheckpoint;
    const cancellationState = { ...original.state, continuationToken: "retained" };
    const cancellationContext = { retained: true };
    current = {
      ...original,
      result: { ...original.result!, cancellationState, cancellationContext },
    };
    records.set("proposal", current);
    await finalizeTurnStep({
      session: resources,
      eventIds: ["initial", "followup"],
      checkpoint: recordRef("proposal"),
      kind: "interrupt",
      pending: [],
    });
    expect(mocks.cancel).toHaveBeenCalledWith(
      expect.objectContaining({
        sessionState: cancellationState,
        serializedContext: cancellationContext,
        settlement: expect.objectContaining({ events: [{ type: "turn.interrupted" }] }),
      }),
    );
    expect(mocks.finalize).not.toHaveBeenCalled();
    expect(mocks.notifyCancel).toHaveBeenCalledWith(
      expect.objectContaining({ lifecycle: "parked" }),
    );
  });

  it("does not repeat terminal effects after an uncommitted finalization attempt", async () => {
    mocks.finalize.mockRejectedValueOnce(new Error("Lost completion"));
    const input = {
      session: resources,
      eventIds: ["initial", "followup"],
      checkpoint: recordRef("proposal"),
      kind: "natural" as const,
      pending: [],
    };
    await expect(finalizeTurnStep(input)).rejects.toThrow("Lost completion");
    await expect(finalizeTurnStep(input)).rejects.toThrow("did not commit its effects");
    expect(mocks.finalize).toHaveBeenCalledTimes(1);
  });

  it("rejects an obsolete proposal before invoking settlement hooks", async () => {
    await expect(
      finalizeTurnStep({
        session: resources,
        eventIds: ["initial"],
        checkpoint: { ...recordRef("proposal"), index: 99 },
        kind: "natural",
        pending: [],
      }),
    ).rejects.toThrow("no longer");
    expect(mocks.read).not.toHaveBeenCalled();
    expect(mocks.append).not.toHaveBeenCalled();
    expect(mocks.finalize).not.toHaveBeenCalled();
  });

  it("recovers cold initialization through a compact failure attempt without a marker chain", async () => {
    const initial = checkpoint();
    records.clear();
    current = {
      phase: "entered",
      writeId: "initialize:entered",
      writerRunId: "owner",
      source: { initial },
    };
    records.set(current.writeId, current);
    const result = await failTurnStep({
      sessionId: resources.sessionId,
      resources,
      eventIds: ["initial"],

      failure: { kind: "execution" as const, error: "initial effects failed" },
    });
    expect(result.terminal).toBe(true);
    expect(records.get("commit:entered")).toMatchObject({ source: { initial } });
    expect(records.get("commit")).toMatchObject({ phase: "terminal", state: initial.state });
    expect(mocks.finalize).toHaveBeenCalledOnce();
  });

  it("lets a separate failure operation recover the exact committed source of an unfinished attempt", async () => {
    const original = current as InitializedSessionCheckpoint;
    mocks.finalize.mockRejectedValueOnce(new Error("Lost completion"));
    await expect(
      finalizeTurnStep({
        session: resources,
        eventIds: ["initial"],
        checkpoint: recordRef("proposal"),
        kind: "natural",
        pending: [],
      }),
    ).rejects.toThrow("Lost completion");
    mocks.stepId = "failure";
    await failTurnStep({
      sessionId: resources.sessionId,
      resources,
      eventIds: ["initial"],

      failure: { kind: "execution" as const, error: "finalization failed" },
    });
    expect(records.get("failure:entered")).toMatchObject({
      source: { ref: recordRef("proposal") },
    });
    expect(records.get("failure")).toMatchObject({ phase: "terminal", state: original.state });
    expect(mocks.read.mock.calls.map(([ref]) => ref)).toContainEqual(recordRef("proposal"));
  });

  it("does not confuse unreadable state with an empty session", async () => {
    mocks.open.mockRejectedValueOnce(new Error("Storage failed"));
    await expect(
      failTurnStep({
        sessionId: resources.sessionId,
        resources,
        eventIds: ["initial", "followup"],

        failure: { kind: "execution" as const, error: "private failure" },
      }),
    ).rejects.toThrow("Storage failed");
    expect(mocks.appendEvents).not.toHaveBeenCalled();
  });

  it.each(["seed", "settled", "other-owner"] as const)(
    "leaves %s state unchanged after exhausted storage reads",
    async (kind) => {
      records.clear();
      const stored: TurnCheckpointRecord =
        kind === "seed"
          ? {
              phase: "seed",
              writeId: "seed",
              submission: { eventId: "initial", command: { kind: "cancel" } },
            }
          : {
              ...checkpoint(),
              phase: kind === "settled" ? "settled" : "running",
              writerRunId: "previous",
            };
      records.set(stored.writeId, stored);
      current = stored;
      await expect(
        failTurnStep({
          sessionId: resources.sessionId,
          resources,
          eventIds: ["followup"],
          failure: { kind: "storage", error: new Error("read failed") },
        }),
      ).rejects.toMatchObject({ name: "SessionStorageUnavailableError" });
      expect(mocks.append).not.toHaveBeenCalled();
      expect(mocks.appendEvents).not.toHaveBeenCalled();
      expect(mocks.cancelRun).not.toHaveBeenCalled();
    },
  );

  it("settles its own durable effects when a later retry only reports a read failure", async () => {
    const original = checkpoint();
    const attempt: TurnCheckpointRecord = {
      phase: "entered",
      writeId: "execute:entered",
      writerRunId: "owner",
      source: { initial: original },
    };
    records.clear();
    records.set(attempt.writeId, attempt);
    current = attempt;
    const result = await failTurnStep({
      sessionId: resources.sessionId,
      resources,
      eventIds: ["initial"],
      failure: { kind: "storage", error: new Error("later read failed") },
    });
    expect(result.terminal).toBe(true);
    expect(records.get("commit")).toMatchObject({ phase: "terminal" });
    expect(mocks.cancelRun).toHaveBeenCalled();
  });

  it("uses the entered marker owner when its source checkpoint belongs to the previous turn", async () => {
    const source = { ...checkpoint(), phase: "settled" as const, writerRunId: "previous" };
    records.clear();
    records.set(source.writeId, source);
    const attempt: TurnCheckpointRecord = {
      phase: "entered",
      writeId: "execute:entered",
      writerRunId: "owner",
      source: { ref: recordRef(source.writeId) },
    };
    records.set(attempt.writeId, attempt);
    current = attempt;
    const result = await failTurnStep({
      sessionId: resources.sessionId,
      resources,
      eventIds: ["initial"],
      failure: { kind: "storage", error: new Error("later read failed") },
    });
    expect(result.terminal).toBe(true);
    expect(records.get("commit")).toMatchObject({ phase: "terminal" });
  });

  it("records initialization failure without inventing a harness snapshot or exposing the error", async () => {
    records.clear();
    current = undefined;
    const input = {
      sessionId: resources.sessionId,
      resources,
      eventIds: ["initial", "followup"],
      submission: { eventId: "initial", command: { kind: "cancel" as const } },
      failure: { kind: "execution" as const, error: "private secret detail" },
    };
    const result = await failTurnStep(input);
    expect(result).toMatchObject({ terminal: true, deliveries: { initial: "retired" } });
    const failed = records.get("commit");
    expect(failed?.phase).toBe("initialization-failed");
    expect(failed).not.toHaveProperty("state");
    expect(JSON.stringify(mocks.appendEvents.mock.calls)).not.toContain("private secret detail");
    expect(mocks.log).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ error: "private secret detail" }),
    );
    await failTurnStep(input);
    expect(mocks.appendEvents).toHaveBeenCalledTimes(1);
  });

  it("does not repeat an uncertain bootstrap failure notification", async () => {
    records.clear();
    current = undefined;
    mocks.appendEvents.mockRejectedValueOnce(new Error("Write completion unknown"));
    const input = {
      sessionId: resources.sessionId,
      resources,
      eventIds: ["initial", "followup"],
      submission: {
        eventId: "initial",
        command: { kind: "send" as const, payload: { message: "Hello" } },
      },
      failure: { kind: "execution" as const, error: "private" },
    };
    await expect(failTurnStep(input)).rejects.toThrow("Write completion unknown");
    await expect(failTurnStep(input)).rejects.toThrow("did not commit its effects");
    expect(mocks.appendEvents).toHaveBeenCalledOnce();
  });
  it("accounts for a cancellation request while retaining undispatched input", () => {
    const original = checkpoint();
    const message = {
      candidateRunId: "candidate",
      submission: {
        eventId: "steer",
        command: { kind: "send" as const, payload: { message: "Keep me" } },
      },
    };
    const cancel = {
      candidateRunId: "cancel-candidate",
      submission: { eventId: "cancel", command: { kind: "cancel" as const } },
    };
    const accounted = accountPending({ ...original, inputs: [message, cancel] }, [], "cancel");
    expect(accounted.queue).toEqual([message]);
    expect(accounted.deliveries.cancel).toBe("applied");
    expect(accounted.inputs).toEqual([]);
  });
});
