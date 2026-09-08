import {
  createStreamStorageScope,
  type StreamStorageScope,
} from "#execution/session/stream-storage.js";
import { selectDeliveries } from "#execution/turn/receipts.js";
import { accountPending } from "#execution/turn/submissions.js";
import type { TurnSettlementKind } from "#execution/turn/types.js";
import { getStepMetadata, getWorkflowMetadata } from "#compiled/@workflow/core/index.js";
import { sessionEvents } from "#execution/session/events.js";
import { sessionSnapshots } from "#execution/session/snapshots.js";
import { openCheckpointLog } from "#execution/turn/checkpoint-log.js";
import { resolveSessionTarget } from "#execution/session/directory.js";
import type {
  SessionResources,
  SessionTarget,
  SnapshotRecordRef,
} from "#execution/session/resources.js";
import type { InboxEnvelope } from "#execution/inbox/types.js";
import type {
  InitializationFailureCheckpoint,
  SessionCheckpoint,
  TurnReceipt,
} from "#execution/turn/types.js";
import { finalizeModelSettlement } from "#execution/turn/finalize-model.js";
import { cancellationSettlement, settleCancelledTurn } from "#execution/turn/cancel.js";
import { cancelDescendantTurns } from "#execution/turn/cancel-descendants.js";
import { terminateChildSessions } from "#execution/turn/terminate-children.js";
import { cancelSessionTimeout } from "#execution/session-timeout-steps.js";
import {
  notifyCancelledTaskCaller,
  notifyDelegatedParent,
  notifyTurnCaller,
} from "#subagents/parent-notification.js";
import {
  createDelegatedSubagentErrorResult,
  createDelegatedSubagentSuccessResult,
} from "#subagents/parent-result.js";
import { fireSessionCallback } from "#subagents/callbacks.js";
import {
  createSessionCompletedEvent,
  createSessionFailedEvent,
  stampMessageStreamEvent,
} from "#protocol/message.js";
import { cancelRun, getWorld } from "#internal/workflow/runtime.js";
import { isTaskWorkflowTargetGone } from "#execution/tasks/workflow-target.js";
import { createLogger } from "#internal/logging.js";
import { notifyInitializationFailure } from "#execution/turn/initialization-failure.js";
import { SessionStorageUnavailableError } from "#execution/session/storage-error.js";
import { toErrorMessage } from "#shared/errors.js";

const log = createLogger("execution.turn.finalize");
const FAILURE_MESSAGE = "The turn could not complete safely.";

interface FinalizeTurnInput {
  readonly eventIds: readonly string[];
  readonly claimedContinuationToken?: string;
  readonly session: SessionResources;
  readonly checkpoint: SnapshotRecordRef;
  readonly kind: TurnSettlementKind;
  readonly pending: readonly InboxEnvelope[];
}

/** The owner calls this only after sealing admission to the completed model turn. */
export async function finalizeTurnStep(input: FinalizeTurnInput): Promise<TurnReceipt> {
  "use step";
  return await finalizeTurn(input, createStreamStorageScope());
}

async function finalizeTurn(
  input: FinalizeTurnInput,
  storage: StreamStorageScope,
  opened?: Awaited<ReturnType<typeof openCheckpointLog>>,
): Promise<TurnReceipt> {
  const snapshots = opened ?? (await openCheckpointLog(input.session.snapshots, storage));
  const writeId = getStepMetadata().stepId;
  const completed = snapshots.completed(writeId);
  if (completed !== undefined) {
    if (isTerminal(completed.checkpoint))
      await closeSession(input.session, completed.checkpoint, storage);
    return receipt(completed.ref, completed.checkpoint, input.eventIds);
  }
  if (snapshots.entered(writeId)) {
    throw new Error("The previous finalization attempt did not commit its effects.");
  }
  if (input.kind !== "failure") snapshots.assertCurrent(input.checkpoint);
  const loaded = (await snapshots.read(input.checkpoint))!.checkpoint;
  if (loaded.phase === "initialization-failed")
    return receipt(input.checkpoint, loaded, input.eventIds);
  const original = accountPending(loaded, input.pending, input.kind);
  const result = original.result;
  const cancelling =
    input.kind === "cancel" || input.kind === "interrupt" || input.kind === "reset";
  const terminal =
    input.kind === "reset" ||
    input.kind === "timeout" ||
    input.kind === "failure" ||
    (input.kind === "natural" && result?.action === "done");
  let checkpoint = cancelling
    ? {
        ...original,
        state: result?.cancellationState ?? original.state,
        serializedContext: result?.cancellationContext ?? original.serializedContext,
      }
    : original;
  let settlement =
    input.kind === "natural" || input.kind === "timeout" ? result?.settlement : undefined;
  if (cancelling) {
    settlement = cancellationSettlement(
      checkpoint.state,
      input.kind === "interrupt" ? "interrupt" : terminal ? "terminal" : "cancel",
    );
  }
  if (terminal && input.kind !== "natural") {
    const event =
      input.kind === "failure"
        ? createSessionFailedEvent({
            sessionId: input.session.sessionId,
            code: "TURN_EXECUTION_FAILED",
            message: FAILURE_MESSAGE,
          })
        : createSessionCompletedEvent();
    settlement = {
      events: [
        ...(settlement?.events ?? []).filter(
          (event) => input.kind !== "timeout" || event.type !== "session.waiting",
        ),
        stampMessageStreamEvent(event),
      ],
      emissionAfter: settlement?.emissionAfter ?? checkpoint.state.emissionState,
    };
  }
  await snapshots.begin(writeId, checkpoint, input.checkpoint);

  const eventStream = sessionEvents.open(input.session.events, storage);
  checkpoint = await eventStream.withWriter(async (events) => {
    let current = checkpoint;
    if (cancelling) {
      await cancelDescendantTurns({
        sessionState: current.state,
        serializedContext: current.serializedContext,
      });
      const settled = await settleCancelledTurn({
        events,
        sessionState: current.state,
        serializedContext: current.serializedContext,
        settlement: settlement!,
      });
      current = {
        ...current,
        state: settled.sessionState,
        serializedContext: settled.serializedContext,
      };
      await notifyCancelledTaskCaller({
        caller: current.caller,
        lifecycle: terminal ? "terminal" : "parked",
        sessionId: input.session.sessionId,
        usage: settled.usage,
      });
      current = { ...current, caller: undefined };
    } else if (settlement !== undefined) {
      const settled = await finalizeModelSettlement({
        events,
        sessionState: current.state,
        serializedContext: current.serializedContext,
        settlement,
      });
      current = {
        ...current,
        state: settled.sessionState,
        serializedContext: settled.serializedContext,
      };
    }
    const outcome =
      result?.action === "done"
        ? { output: result.output ?? "", isError: result.isError, usage: result.usageDelta }
        : result?.action === "park"
          ? result.settled
          : undefined;
    if ((input.kind === "natural" || input.kind === "timeout") && outcome !== undefined) {
      await notifyTurnCaller({
        caller: current.caller,
        lifecycle: terminal ? "terminal" : "parked",
        sessionId: input.session.sessionId,
        settled: outcome,
      });
      current = { ...current, caller: undefined };
    } else if (input.kind === "failure" && current.caller !== undefined) {
      await notifyTurnCaller({
        caller: current.caller,
        lifecycle: "terminal",
        sessionId: input.session.sessionId,
        settled: { isError: true, output: FAILURE_MESSAGE },
      });
      current = { ...current, caller: undefined };
    }
    if (terminal) {
      await terminateChildSessions({
        sessionState: current.state,
        serializedContext: current.serializedContext,
      });
      if (current.serializedContext["eve.mode"] === "task") {
        const failed =
          input.kind !== "natural" || (result?.action === "done" && result.isError === true);
        const output =
          input.kind === "natural" && result?.action === "done" ? result.output : FAILURE_MESSAGE;
        const usage = result?.action === "done" ? result.usage : undefined;
        await fireSessionCallback({
          serializedContext: current.serializedContext,
          status: failed ? "failed" : "completed",
          output: failed ? undefined : output,
          error: failed ? output : undefined,
          usage,
        });
        await notifyDelegatedParent({
          serializedContext: current.serializedContext,
          result: failed
            ? createDelegatedSubagentErrorResult(current.serializedContext, output)
            : createDelegatedSubagentSuccessResult(current.serializedContext, output),
          usage,
        });
      }
    }
    return current;
  });

  const deliveries = { ...checkpoint.deliveries };
  if (terminal)
    for (const item of checkpoint.queue) deliveries[item.submission.eventId] = "retired";
  checkpoint = {
    ...checkpoint,
    writeId,
    phase: terminal ? "terminal" : "settled",
    claimedContinuationToken: input.claimedContinuationToken ?? checkpoint.claimedContinuationToken,
    deliveries,
    queue: terminal ? [] : checkpoint.queue,
    inputs: [],
    result: undefined,
  };
  const ref = await snapshots.commit(checkpoint);
  if (terminal) await closeSession(input.session, checkpoint, storage);
  return receipt(ref, checkpoint, input.eventIds);
}

export async function failTurnStep(
  target: SessionTarget & {
    readonly eventIds: readonly string[];
    readonly checkpoint?: SnapshotRecordRef;
    readonly failure: { readonly kind: "storage" | "execution"; readonly error: unknown };
  },
): Promise<TurnReceipt> {
  "use step";
  const storage = createStreamStorageScope();
  const session = await resolveSessionTarget(target, storage);
  const input = {
    ...target,
    session,
    eventIds: [...new Set([...target.eventIds, session.initialEventId])],
  };
  log.error("Turn execution failed", {
    sessionId: input.session.sessionId,
    error: toErrorMessage(input.failure.error),
  });
  const writeId = getStepMetadata().stepId;
  const snapshots = await openCheckpointLog(input.session.snapshots, storage);
  const completed = snapshots.completed(writeId);
  if (completed !== undefined) {
    await closeSession(input.session, completed.checkpoint, storage);
    return receipt(completed.ref, completed.checkpoint, input.eventIds);
  }
  const entered = snapshots.completed(`${writeId}:entered`);
  if (entered?.checkpoint.phase === "initialization-failed") {
    await closeSession(input.session, entered.checkpoint, storage);
    throw new Error("The previous initialization failure notification did not commit its effects.");
  }
  const latest = await snapshots.read(input.checkpoint);
  if (
    input.failure.kind === "storage" &&
    snapshots.effectsOwnerRunId !== getWorkflowMetadata().workflowRunId
  ) {
    throw new SessionStorageUnavailableError(input.failure.error);
  }
  if (latest !== undefined && latest.checkpoint.phase !== "initialization-failed") {
    return await finalizeTurn(
      {
        session: input.session,
        checkpoint: latest.ref,
        kind: "failure",
        eventIds: input.eventIds,
        pending: [],
      },
      storage,
      snapshots,
    );
  }
  const bootstrap = snapshots.bootstrap;
  const failed: InitializationFailureCheckpoint =
    latest?.checkpoint.phase === "initialization-failed"
      ? latest.checkpoint
      : {
          writeId: `${writeId}:entered`,
          writerRunId: getWorkflowMetadata().workflowRunId,
          phase: "initialization-failed",
          deliveries: Object.fromEntries(input.eventIds.map((id) => [id, "retired" as const])),
          queue: [],
          event: stampMessageStreamEvent(
            createSessionFailedEvent({
              sessionId: input.session.sessionId,
              code: "SESSION_INITIALIZATION_FAILED",
              message: "The session could not initialize.",
            }),
          ),
        };
  const enteringRef =
    latest?.checkpoint.phase === "initialization-failed"
      ? latest.ref
      : await snapshots.commit(failed);
  if (failed.writeId !== `${writeId}:entered`) {
    await closeSession(input.session, failed, storage);
    return receipt(enteringRef, failed, input.eventIds);
  }
  await sessionEvents.open(input.session.events, storage).append([failed.event]);
  await notifyInitializationFailure({
    event: failed.event,
    serializedContext: {
      ...bootstrap?.initial?.serializedContext,
      "eve.sessionId": input.session.sessionId,
    },
  });
  const committed: InitializationFailureCheckpoint = { ...failed, writeId };
  const ref = await snapshots.commit(committed);
  await closeSession(input.session, committed, storage);
  return receipt(ref, committed, input.eventIds);
}

function isTerminal(checkpoint: SessionCheckpoint): boolean {
  return checkpoint.phase === "terminal" || checkpoint.phase === "initialization-failed";
}

async function closeSession(
  session: SessionResources,
  checkpoint: SessionCheckpoint,
  storage: StreamStorageScope,
): Promise<void> {
  await sessionEvents.open(session.events, storage).close();
  await sessionSnapshots.close(session.snapshots, storage);
  if (checkpoint.phase !== "initialization-failed" && checkpoint.timeoutRunId !== undefined) {
    await cancelSessionTimeout({ runId: checkpoint.timeoutRunId });
  }
  const collector =
    checkpoint.phase === "initialization-failed" ? undefined : checkpoint.activityCollectorRunId;
  for (const runId of [collector, session.holderRunId]) {
    if (runId === undefined) continue;
    try {
      await cancelRun(await getWorld(), runId);
    } catch (error) {
      if (!isTaskWorkflowTargetGone(error)) throw error;
    }
  }
}

function receipt(
  ref: SnapshotRecordRef,
  checkpoint: SessionCheckpoint,
  eventIds: readonly string[],
): TurnReceipt {
  const terminal = isTerminal(checkpoint);
  return {
    checkpoint: ref,
    deliveries: selectDeliveries(checkpoint.deliveries, eventIds),
    terminal,
    ...(!terminal && checkpoint.phase !== "initialization-failed"
      ? { continuationToken: checkpoint.state.snapshot.session.continuationToken }
      : {}),
  };
}
