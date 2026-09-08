import {
  admitSubmissions,
  splitSubmission,
  retireTaskSubmissions,
} from "#execution/turn/submissions.js";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { executeTurnStep, projectProgress } from "#execution/turn/execute.js";
import { setEveAttributes } from "#runtime/attributes/emit.js";
import {
  createDurableSessionState,
  replaceDurableSessionSnapshot,
} from "#execution/session/state.js";
import { createSessionResources, type SnapshotRecordRef } from "#execution/session/resources.js";
import type { SnapshotLog } from "#execution/session/snapshots.js";
import type { TurnCheckpointRecord } from "#execution/turn/checkpoint-log.js";
import { recordWorkflowToolRun } from "#harness/workflow-tool-runs.js";
import { getAgentHandleStore, writeHandles } from "#subagents/handles/store.js";
import type {
  AcceptedSubmission,
  InitializedSessionCheckpoint,
  PendingSubmission,
} from "#execution/turn/types.js";

const mocks = vi.hoisted(() => ({
  open: vi.fn(),
  head: vi.fn(),
  read: vi.fn(),
  append: vi.fn(),
  model: vi.fn(),
  runtime: vi.fn(),
  route: vi.fn(),
  dispatch: vi.fn(),
  acknowledge: vi.fn(),
  acknowledgeTools: vi.fn(),
  create: vi.fn(),
  resolveSession: vi.fn(),
}));
vi.mock("#compiled/@workflow/core/index.js", () => ({
  getStepMetadata: () => ({ stepId: "step" }),
}));
vi.mock("#runtime/attributes/emit.js", () => ({ setEveAttributes: vi.fn() }));
vi.mock("#execution/session/snapshots.js", () => ({
  sessionSnapshots: {
    open: mocks.open,
  },
}));
vi.mock("#execution/session/directory.js", () => ({
  resolveSessionTarget: mocks.resolveSession,
}));
vi.mock("#execution/session/events.js", () => ({
  sessionEvents: {
    open: () => ({
      withWriter: async (run: (stream: WritableStream<Uint8Array>) => unknown) =>
        run(new WritableStream()),
    }),
  },
}));
vi.mock("#execution/session/create-state.js", () => ({ createSessionState: mocks.create }));
vi.mock("#execution/turn/dispatch-coordination.js", () => ({
  dispatchCoordination: mocks.dispatch,
}));
vi.mock("#execution/tasks/dispatch.js", () => ({ acknowledgeDelegatedTasks: mocks.acknowledge }));
vi.mock("#execution/workflow-tool/start.js", () => ({
  acknowledgeWorkflowTools: mocks.acknowledgeTools,
}));
vi.mock("#execution/route-child-delivery.js", () => ({ routeDeliverToChildren: mocks.route }));
vi.mock("#execution/turn/model.js", () => ({ runModel: mocks.model }));
vi.mock("#execution/turn/runtime-events.js", () => ({ applyRuntimeEvents: mocks.runtime }));
vi.mock("#subagents/parent-notification.js", () => ({
  bindTurnCallerContext: async (input: { serializedContext: unknown }) => input.serializedContext,
  resolveInitialTurnCaller: async () => undefined,
}));
vi.mock("#execution/session-timeout-steps.js", () => ({ startSessionTimeout: vi.fn() }));

const session = createSessionResources("holder", "first", {
  runId: "holder",
  deploymentId: "owner-deployment",
});
const ref: SnapshotRecordRef = { streamId: session.snapshots.id, index: 1 };
const owner = { token: "inbox", ownerRunId: "candidate" };
const submission: AcceptedSubmission = {
  eventId: "next",
  command: { kind: "send", payload: { message: "Continue" } },
};
let checkpoint: InitializedSessionCheckpoint;

beforeEach(() => {
  vi.clearAllMocks();
  mocks.resolveSession.mockResolvedValue(session);
  const state = createDurableSessionState({
    session: {
      sessionId: session.sessionId,
      continuationToken: "alias",
      history: [],
      agent: { modelReference: { id: "model" }, system: "", tools: [] },
      compaction: { threshold: 1000, recentWindowSize: 10 },
    },
  });
  checkpoint = {
    writeId: "before",
    writerRunId: "previous",
    phase: "settled",
    state,
    serializedContext: {},
    deliveries: {},
    queue: [],
  };
  mocks.head.mockImplementation(() => ({ ref, checkpoint }));
  mocks.open.mockImplementation(async () => {
    let latest = mocks.head() as SnapshotLog<TurnCheckpointRecord>["latest"];
    const records = new Map<number, TurnCheckpointRecord>();
    if (latest !== undefined) records.set(latest.ref.index, latest.checkpoint);
    mocks.read.mockImplementation(async (target: SnapshotRecordRef) => {
      const value = records.get(target.index);
      if (target.streamId !== session.snapshots.id || value === undefined)
        throw new Error("Unknown snapshot reference.");
      return value;
    });
    mocks.append.mockImplementation(async (value: TurnCheckpointRecord) => {
      const written = { streamId: session.snapshots.id, index: (latest?.ref.index ?? 0) + 1 };
      records.set(written.index, value);
      latest = { ref: written, checkpoint: value };
      return written;
    });
    return {
      get latest() {
        return latest;
      },
      read: mocks.read,
      append: mocks.append,
    } satisfies SnapshotLog<TurnCheckpointRecord>;
  });
  mocks.create.mockResolvedValue({ state });
  mocks.model.mockImplementation(async (input) => ({
    action: "continue",
    sessionState: input.sessionState,
    serializedContext: input.serializedContext,
  }));
  mocks.route.mockImplementation(async (input) => ({
    kind: "continue",
    inputChanged: false,
    remainder: input.delivery,
    sessionState: input.sessionState,
    serializedContext: input.serializedContext,
  }));
  mocks.runtime.mockImplementation(async (input) => ({
    inputChanged: false,
    state: input.state,
    serializedContext: input.serializedContext,
    results: [],
    acceptedAtMsByCallId: {},
  }));
});

const run = (changes: Partial<Parameters<typeof executeTurnStep>[0]> = {}) =>
  executeTurnStep({
    sessionId: session.sessionId,
    owner,
    submission,
    work: { kind: "model" },
    abortSignal: new AbortController().signal,
    ...changes,
  }).then((executed) => executed.result);

describe("turn execution boundary", () => {
  it("projects and cancels a persisted task without a caller", () => {
    checkpoint = {
      ...checkpoint,
      phase: "running",
      caller: undefined,
      state: replaceDurableSessionSnapshot({
        session: { ...checkpoint.state.snapshot.session, taskId: "standalone" },
      }),
    };
    expect(projectProgress(ref, checkpoint).taskId).toBe("standalone");
    const cancelled = retireTaskSubmissions(checkpoint, {
      eventId: "cancel",
      command: { kind: "cancel", taskId: "standalone" },
    });
    expect(cancelled.deliveries.cancel).toBe("applied");
  });
  it("does not hydrate or fail the session when its resource descriptor is unavailable", async () => {
    mocks.resolveSession.mockRejectedValueOnce(new Error("Descriptor not ready"));
    await expect(run()).rejects.toMatchObject({ name: "SessionStorageUnavailableError" });
    expect(mocks.open).not.toHaveBeenCalled();
    expect(mocks.create).not.toHaveBeenCalled();
    expect(mocks.model).not.toHaveBeenCalled();
  });

  it("rejects a missing stored seed instead of initializing from candidate input", async () => {
    mocks.head.mockReturnValue(undefined);
    await expect(
      run({ submission: { ...submission, eventId: "first", initial: { serializedContext: {} } } }),
    ).rejects.toThrow("bootstrap submission is missing");
    expect(mocks.create).not.toHaveBeenCalled();
  });
  it("starts attributes before the marker and runs the model without waiting for them", async () => {
    const attributes = Promise.withResolvers<void>();
    const marker = Promise.withResolvers<void>();
    vi.mocked(setEveAttributes).mockImplementationOnce(() => attributes.promise);
    mocks.append.mockImplementationOnce(async () => {
      marker.resolve();
      return { streamId: session.snapshots.id, index: 2 };
    });
    const result = run();
    await marker.promise;
    expect(setEveAttributes).toHaveBeenCalledOnce();
    await result;
    expect(mocks.model).toHaveBeenCalledOnce();
    attributes.resolve();
  });

  it("reports a marker failure without waiting for an in-flight attribute write", async () => {
    const attributes = Promise.withResolvers<void>();
    const marker = Promise.withResolvers<void>();
    const failure = new Error("Marker write failed");
    vi.mocked(setEveAttributes).mockImplementationOnce(() => attributes.promise);
    mocks.append.mockImplementationOnce(async () => {
      marker.resolve();
      throw failure;
    });
    const result = run().catch((error: unknown) => error);
    await marker.promise;
    expect(await result).toBe(failure);
    expect(mocks.model).not.toHaveBeenCalled();
    attributes.resolve();
  });

  it("hydrates the persisted bootstrap seed and commits before model effects", async () => {
    const firstTurn: AcceptedSubmission = {
      ...submission,
      eventId: "first",
      initial: { serializedContext: { "eve.bundle": { source: {} } }, sessionTimeoutMs: false },
    };
    mocks.head.mockReturnValue({
      ref: { ...ref, index: 0 },
      checkpoint: { phase: "seed", writeId: "first", submission: firstTurn },
    });
    const result = await run({ submission: firstTurn });
    expect(mocks.append.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.model.mock.invocationCallOrder[0]!,
    );
    expect(result).toMatchObject({
      kind: "progress",
      progress: { checkpoint: { streamId: session.snapshots.id, index: 2 } },
    });
    expect(JSON.stringify(result)).not.toContain("serializedContext");
    expect(JSON.stringify(result)).not.toContain("history");
  });

  it("does not replay model effects after a committed attempt, but retries its durable task acknowledgment", async () => {
    const task = { taskId: "task", taskRunId: "run", taskInboxToken: "task-inbox" };
    mocks.head.mockReturnValueOnce({
      ref,
      checkpoint: {
        ...checkpoint,
        writeId: "step",
        pendingTaskAcks: [task],
        result: { action: "continue" },
      },
    });
    await run();
    expect(mocks.model).not.toHaveBeenCalled();
    expect(mocks.acknowledge).toHaveBeenCalledWith({ tasks: [task] });
    expect(mocks.append).not.toHaveBeenCalled();
  });

  it("fails a previously entered attempt without repeating uncertain effects", async () => {
    mocks.head.mockReturnValueOnce({
      ref: { ...ref, index: 2 },
      checkpoint: {
        phase: "entered",
        writeId: "step:entered",
        writerRunId: owner.ownerRunId,
        source: { ref },
      },
    });
    await expect(run()).rejects.toThrow("did not commit");
    expect(mocks.model).not.toHaveBeenCalled();
  });

  it("rejects another owner's unfinished attempt before hydrating its prior state", async () => {
    mocks.head.mockReturnValueOnce({
      ref: { ...ref, index: 2 },
      checkpoint: {
        phase: "entered",
        writeId: "unfinished:entered",
        writerRunId: "previous-owner",
        source: { ref },
      },
    });
    await expect(run()).rejects.toThrow("without settling its effects");
    expect(mocks.read).not.toHaveBeenCalled();
    expect(mocks.append).not.toHaveBeenCalled();
    expect(mocks.model).not.toHaveBeenCalled();
  });

  it("rejects an obsolete boundary reference before beginning model effects", async () => {
    await expect(run({ checkpoint: { ...ref, index: 2 } })).rejects.toThrow("no longer");
    expect(mocks.read).not.toHaveBeenCalled();
    expect(mocks.append).not.toHaveBeenCalled();
    expect(mocks.model).not.toHaveBeenCalled();
  });

  it("keeps a pending earlier candidate in front without touching session effects", async () => {
    checkpoint = {
      ...checkpoint,
      queue: [{ submission: { ...submission, eventId: "earlier" }, candidateRunId: "earlier-run" }],
    };
    expect(await run()).toEqual({ kind: "wait", runId: "earlier-run" });
    expect(mocks.append).not.toHaveBeenCalled();
    expect(mocks.model).not.toHaveBeenCalled();
  });

  it("does not turn a storage failure into an empty session", async () => {
    mocks.open.mockRejectedValueOnce(new Error("storage unavailable"));
    await expect(run()).rejects.toMatchObject({ name: "SessionStorageUnavailableError" });
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("retires a cancel that acquires idle ownership without inventing a model turn", async () => {
    await run({ submission: { eventId: "cancel", command: { kind: "cancel", turnId: "old" } } });
    expect(mocks.model).not.toHaveBeenCalled();
    expect(mocks.append.mock.lastCall?.[0]).toMatchObject({
      deliveries: { cancel: "retired" },
      result: { action: "park" },
    });
  });
  it("accepts expiry of an idle session without inventing a cancelled model turn", async () => {
    await run({ submission: { eventId: "expiry", command: { kind: "session-timeout" } } });
    expect(mocks.model).not.toHaveBeenCalled();
    expect(mocks.append.mock.lastCall?.[0]).toMatchObject({
      deliveries: { expiry: "applied" },
      result: { action: "park" },
      inputs: [],
    });
  });

  it.each(["park", "continue"] as const)(
    "does not let a duplicate submission change the model's %s decision",
    async (action) => {
      checkpoint = {
        ...checkpoint,
        phase: "running",
        writerRunId: owner.ownerRunId,
        deliveries: { [submission.eventId]: "applied" },
        inputs: [],
        result: {
          action,
          hasPendingAuthorization: false,
          hasPendingInputBatch: false,
          sessionState: checkpoint.state,
          serializedContext: checkpoint.serializedContext,
        },
      };
      const result = await run({
        checkpoint: ref,
        work: {
          kind: "model",
          envelopes: [
            {
              eventId: submission.eventId,
              kind: "session.submit",
              payload: { submission, candidateRunId: "concurrent-candidate" },
            },
          ],
        },
      });
      expect(mocks.model).toHaveBeenCalledTimes(action === "continue" ? 1 : 0);
      expect(mocks.append.mock.lastCall?.[0]).toMatchObject({
        deliveries: { [submission.eventId]: "applied" },
        inputs: [],
        result: { action },
      });
      expect(result).toMatchObject({
        kind: "progress",
        progress: { action: action === "continue" ? "continue" : "settle" },
      });
    },
  );

  it("advances the active step when steering replaces an unpublished settlement", async () => {
    const emission = { sessionStarted: true, sequence: 2, stepIndex: 3, turnId: "turn_owner" };
    const state = replaceDurableSessionSnapshot({
      session: {
        ...checkpoint.state.snapshot.session,
        state: { "eve.harness.emission": emission },
      },
    });
    checkpoint = {
      ...checkpoint,
      phase: "running",
      writerRunId: owner.ownerRunId,
      state,
      inputs: [],
      result: {
        action: "park",
        hasPendingAuthorization: false,
        hasPendingInputBatch: false,
        sessionState: state,
        serializedContext: {},
        settlement: {
          events: [],
          emissionAfter: { ...emission, sequence: 3, stepIndex: 0, turnId: "" },
        },
      },
    };

    await run({
      checkpoint: ref,
      work: {
        kind: "model",
        envelopes: [
          {
            kind: "session.submit",
            eventId: submission.eventId,
            payload: { submission, candidateRunId: "steering-candidate" },
          },
        ],
      },
    });
    expect(mocks.model).toHaveBeenCalledOnce();
    expect(mocks.model.mock.lastCall?.[0]).toMatchObject({
      input: { kind: "deliver", payloads: [{ message: "Continue" }] },
      sessionState: { emissionState: { ...emission, stepIndex: 4 } },
    });
    expect(mocks.append.mock.lastCall?.[0].result).not.toHaveProperty("settlement");
  });

  it("commits executor ownership before acknowledging blocking tools", async () => {
    checkpoint = {
      ...checkpoint,
      phase: "running",
      writerRunId: owner.ownerRunId,
      result: {
        action: "park",
        cancellationState: checkpoint.state,
        cancellationContext: { beforeDispatch: true },
        hasPendingAuthorization: false,
        hasPendingInputBatch: false,
        pendingCoordinationCallIds: ["call"],
        sessionState: checkpoint.state,
        serializedContext: {},
      },
    };
    const tool = { callId: "call", hookToken: "tool-inbox", runId: "tool-run", toolName: "tool" };
    const dispatchedState = replaceDurableSessionSnapshot({
      session: recordWorkflowToolRun(checkpoint.state.snapshot.session, tool),
    });
    mocks.dispatch.mockResolvedValue({
      sessionState: dispatchedState,
      results: [],
    });
    await run({ checkpoint: ref, work: { kind: "dispatch" } });
    expect(mocks.append.mock.lastCall?.[0]).toMatchObject({
      pendingToolAcks: [tool],
    });
    const committed: InitializedSessionCheckpoint = mocks.append.mock.lastCall?.[0];
    expect(committed.result).not.toHaveProperty("cancellationState");
    expect(committed.result).not.toHaveProperty("cancellationContext");
    expect(committed.result?.cancellationState ?? committed.state).toEqual(dispatchedState);
    expect(mocks.acknowledgeTools).toHaveBeenCalledWith({ runs: [tool] });
    expect(mocks.append.mock.invocationCallOrder[1]).toBeLessThan(
      mocks.acknowledgeTools.mock.invocationCallOrder[0]!,
    );
    expect(mocks.append.mock.invocationCallOrder[1]).toBeLessThan(
      mocks.acknowledge.mock.invocationCallOrder[0]!,
    );
  });

  it("retains committed child admission and context when cancelling after runtime progress", async () => {
    const before = checkpoint.state;
    checkpoint = {
      ...checkpoint,
      phase: "running",
      writerRunId: owner.ownerRunId,
      result: {
        action: "park",
        cancellationState: before,
        cancellationContext: { beforeInvocation: true },
        hasPendingAuthorization: false,
        hasPendingInputBatch: false,
        pendingCoordinationCallIds: ["call"],
        sessionState: before,
        serializedContext: {},
      },
      dispatched: true,
    };
    const handle = {
      phase: "claimed",
      ownerId: "tool-run",
      operationId: "invocation",
      callId: "child-call",
      identity: { id: "agent-id", name: "worker", nodeId: "subagents/worker" },
      address: { kind: "agent/local", sessionId: "child", continuationToken: "child-alias" },
    } as const;
    const state = replaceDurableSessionSnapshot({
      session: writeHandles(before.snapshot.session, [handle]),
    });
    const serializedContext = { afterInvocation: true };
    mocks.runtime.mockResolvedValue({
      state,
      serializedContext,
      results: [],
      acceptedAtMsByCallId: {},
    });

    await run({ checkpoint: ref, work: { kind: "events", envelopes: [] } });
    const committed: InitializedSessionCheckpoint = mocks.append.mock.lastCall?.[0];
    const cancellationState = committed.result?.cancellationState ?? committed.state;
    expect(getAgentHandleStore(cancellationState.snapshot.session.state)?.handles).toEqual([
      handle,
    ]);
    expect(committed.result?.cancellationContext ?? committed.serializedContext).toEqual(
      serializedContext,
    );
  });

  it("retains the current model rollback until another execution boundary admits it", async () => {
    const cancellationState = checkpoint.state;
    const cancellationContext = { retained: true };
    mocks.model.mockResolvedValue({
      action: "continue",
      cancellationState,
      cancellationContext,
      sessionState: checkpoint.state,
      serializedContext: {},
    });
    await run();
    const committed: InitializedSessionCheckpoint = mocks.append.mock.lastCall?.[0];
    expect(committed.result).toMatchObject({ cancellationState, cancellationContext });
  });

  it.each([
    { kind: "runtime", inputChanged: true },
    { kind: "runtime", inputChanged: false },
    { kind: "task", inputChanged: true },
    { kind: "task", inputChanged: false },
    { kind: "answer", inputChanged: true },
  ] as const)(
    "settles idle $kind traffic with a waiting event only when inputChanged=$inputChanged",
    async ({ kind, inputChanged }) => {
      const state = { ...checkpoint.state, hasProxyInputRequests: true };
      mocks.runtime.mockResolvedValue({
        inputChanged: kind === "runtime" && inputChanged,
        state,
        serializedContext: { retained: true },
        results: [],
        acceptedAtMsByCallId: {},
      });
      mocks.route.mockImplementation(async (input) => ({
        kind: "continue",
        inputChanged,
        remainder: undefined,
        sessionState: input.sessionState,
        serializedContext: input.serializedContext,
      }));
      const result = await run({
        submission: {
          eventId: "progress",
          command:
            kind === "runtime"
              ? { kind: "runtime", payload: { kind: "runtime-action-result", results: [] } }
              : {
                  kind: "send",
                  payload:
                    kind === "answer"
                      ? { inputResponses: [{ requestId: "first", text: "yes" }] }
                      : { task: { views: [] } },
                },
        },
      });
      expect(result).toMatchObject({ kind: "progress", progress: { action: "settle" } });
      expect(mocks.model).not.toHaveBeenCalled();
      const committed: InitializedSessionCheckpoint = mocks.append.mock.lastCall?.[0];
      expect(committed.state.hasProxyInputRequests).toBe(true);
      expect(committed.serializedContext).toEqual({ retained: true });
      if (inputChanged) {
        expect(committed.result?.settlement?.events.map((event) => event.type)).toEqual([
          "session.waiting",
        ]);
        expect(committed.result?.settlement?.emissionAfter).toEqual(state.emissionState);
      } else {
        expect(committed.result?.settlement).toBeUndefined();
      }
    },
  );

  it("routes answers to a waiting child without running the model or losing queued messages", async () => {
    checkpoint = {
      ...checkpoint,
      phase: "running",
      writerRunId: owner.ownerRunId,
      inputs: [],
      result: {
        action: "park",
        hasPendingAuthorization: false,
        hasPendingInputBatch: false,
        pendingCoordinationCallIds: ["call"],
        sessionState: checkpoint.state,
        serializedContext: {},
      },
      dispatched: true,
    };
    mocks.route.mockImplementation(async (input) => ({
      kind: "continue",
      inputChanged: true,
      remainder: undefined,
      sessionState: input.sessionState,
      serializedContext: input.serializedContext,
    }));
    const mixed = {
      ...submission,
      command: {
        kind: "send" as const,
        turnPolicy: "queue" as const,
        payload: { message: "Later", inputResponses: [{ requestId: "question", text: "Yes" }] },
      },
    };
    await run({
      checkpoint: ref,
      work: {
        kind: "events",
        envelopes: [
          {
            kind: "session.submit",
            eventId: mixed.eventId,
            payload: { submission: mixed, candidateRunId: "waiting" },
          },
        ],
      },
    });
    expect(mocks.model).not.toHaveBeenCalled();
    expect(mocks.route).toHaveBeenCalledOnce();
    expect(mocks.append.mock.lastCall?.[0].result?.settlement).toBeUndefined();
    expect(mocks.append.mock.lastCall?.[0]).toMatchObject({
      deliveries: { "next:response": "applied" },
      inputs: [],
      queue: [
        {
          candidateRunId: "waiting",
          submission: { eventId: "next", command: { payload: { message: "Later" } } },
        },
      ],
    });
    expect(
      mocks.append.mock.lastCall?.[0].queue[0].submission.command.payload.inputResponses,
    ).toBeUndefined();
  });
});

describe("admission and progress", () => {
  const pending = (command: AcceptedSubmission["command"]): PendingSubmission => ({
    candidateRunId: "other",
    submission: { eventId: "incoming", command },
  });
  it("queues a new invocation caller instead of dropping its reply target into an unrelated turn", () => {
    const caller = {
      callId: "child-call",
      subagentName: "child",
      replyTo: { kind: "session" as const, token: "parent" },
    };
    const input = pending({ kind: "send", payload: { message: "Work" }, caller });
    expect(admitSubmissions(checkpoint, [input])).toMatchObject({ inputs: [], queue: [input] });
  });
  it("keeps mixed answers separate from the queued message's delivery identity", () => {
    const input = pending({
      kind: "send",
      turnPolicy: "queue",
      payload: { message: "Later", inputResponses: [{ requestId: "question", text: "Yes" }] },
    });
    const split = splitSubmission(input);
    expect(split.map((item) => item.submission.eventId)).toEqual(["incoming:response", "incoming"]);
    expect(split.every((item) => item.candidateRunId === input.candidateRunId)).toBe(true);
  });
  it("keeps task-mode human input waits owned until a response can resume the model", () => {
    checkpoint = {
      ...checkpoint,
      result: {
        action: "park",
        hasPendingAuthorization: false,
        hasPendingInputBatch: true,
        sessionState: checkpoint.state,
        serializedContext: {},
      },
    };
    expect(projectProgress(ref, checkpoint).action).toBe("wait");
    checkpoint = {
      ...checkpoint,
      inputs: [
        pending({
          kind: "send",
          payload: { inputResponses: [{ requestId: "question", text: "Yes" }] },
        }),
      ],
    };
    expect(projectProgress(ref, checkpoint).action).toBe("continue");
  });
  it("does not infer alias ownership from the session's current token", () => {
    expect(projectProgress(ref, checkpoint)).toMatchObject({
      continuationToken: "alias",
      claimedContinuationToken: undefined,
    });
  });
  it("keeps the acknowledged alias separate from a new unclaimed continuation", () => {
    checkpoint = { ...checkpoint, claimedContinuationToken: "previous-alias" };
    expect(projectProgress(ref, checkpoint)).toMatchObject({
      continuationToken: "alias",
      claimedContinuationToken: "previous-alias",
    });
  });
  it.each([
    ["cancelled", "cancelled"],
    ["done", "settle"],
  ] as const)("settles %s before consuming remaining inputs", (action, expected) => {
    checkpoint = {
      ...checkpoint,
      inputs: [pending({ kind: "send", payload: { message: "Arrived during execution" } })],
      result: {
        action,
        sessionState: checkpoint.state,
        serializedContext: {},
      },
    };
    expect(projectProgress(ref, checkpoint).action).toBe(expected);
  });
  it("does not resume a blocking batch after only one result", () => {
    checkpoint = {
      ...checkpoint,
      dispatched: true,
      result: {
        action: "park",
        hasPendingAuthorization: false,
        hasPendingInputBatch: false,
        pendingCoordinationCallIds: ["one", "two"],
        sessionState: checkpoint.state,
        serializedContext: {},
      },
      runtimeResults: [{ kind: "tool-result", callId: "one", toolName: "tool", output: "done" }],
    };
    expect(projectProgress(ref, checkpoint).action).toBe("wait");
  });
});
