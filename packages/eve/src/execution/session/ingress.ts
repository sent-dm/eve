import { randomUUID } from "node:crypto";
import { getHookByToken, type Run } from "#internal/workflow/runtime.js";
import type { AcceptedSubmission, TurnReceipt } from "#execution/turn/types.js";
import type { SessionTarget } from "#execution/session/resources.js";
import { dispatchTurn } from "#execution/session/dispatch.js";
import { withWorkflowStartContext } from "#execution/workflow-start.js";
import { readSessionIdFromCommandToken } from "#execution/session-command-token.js";

export interface DispatchedSubmission {
  readonly eventId: string;
  readonly sessionId: string;
  readonly run: Run<TurnReceipt>;
}

/** Freezes the accepted delivery identity and deployment before durable dispatch. */
export function acceptSubmission(
  command: AcceptedSubmission["command"],
  eventId?: string,
): AcceptedSubmission {
  const delivery = command.kind === "send" ? command.delivery : undefined;
  return {
    command,
    eventId:
      eventId ??
      (command.kind === "send" ? (command.taskDeliveryId ?? delivery?.deliveryId) : undefined) ??
      randomUUID(),
    acceptedDeploymentId:
      delivery?.acceptedDeploymentId ?? (process.env.VERCEL_DEPLOYMENT_ID?.trim() || undefined),
  };
}

/** Accepts durable work without reading the holder or session state. */
export async function dispatchAcceptedSubmission(
  session: SessionTarget,
  submission: AcceptedSubmission,
): Promise<DispatchedSubmission> {
  const run = await withWorkflowStartContext(() => dispatchTurn(session, submission));
  return { eventId: submission.eventId, sessionId: session.sessionId, run };
}

/** Starts an existing session's candidate immediately; the owner resolves its resources. */
export async function dispatchSessionCommand(
  sessionId: string,
  command: AcceptedSubmission["command"],
  eventId?: string,
): Promise<DispatchedSubmission> {
  const submission = acceptSubmission(command, eventId);
  return await dispatchAcceptedSubmission({ sessionId }, submission);
}

/** Continuation hooks are lookup addresses; accepted input is sent directly to a turn. */
export async function dispatchSessionCommandByToken(
  token: string,
  command: AcceptedSubmission["command"],
  eventId?: string,
): Promise<DispatchedSubmission> {
  const sessionId = readSessionIdFromCommandToken(token);
  if (sessionId !== undefined) return await dispatchSessionCommand(sessionId, command, eventId);
  const submission = acceptSubmission(command, eventId);
  const hook = await getHookByToken(token);
  return await dispatchAcceptedSubmission({ sessionId: hook.runId }, submission);
}
