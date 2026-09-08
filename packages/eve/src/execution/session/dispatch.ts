import { start, type Run } from "#internal/workflow/runtime.js";
import type { SessionResources, SessionTarget } from "#execution/session/resources.js";
import type { AcceptedSubmission, TurnReceipt, TurnWorkflowInput } from "#execution/turn/types.js";

import { turnWorkflowReference } from "#execution/workflow-references.js";

export async function dispatchTurn(
  session: SessionTarget,
  submission: AcceptedSubmission,
  predecessor?: TurnWorkflowInput["predecessor"],
): Promise<Run<TurnReceipt>> {
  const input: TurnWorkflowInput = {
    ...session,
    submission,
    predecessor,
  };
  const run =
    submission.acceptedDeploymentId === undefined
      ? await start(turnWorkflowReference, [input])
      : await start(turnWorkflowReference, [input], {
          deploymentId: submission.acceptedDeploymentId,
        });
  return run as Run<TurnReceipt>;
}

export async function deferTurnStep(
  input: TurnWorkflowInput & {
    readonly predecessor: NonNullable<TurnWorkflowInput["predecessor"]>;
  },
): Promise<TurnReceipt> {
  "use step";
  const run = await dispatchTurn(
    { sessionId: input.sessionId, resources: input.resources },
    input.submission,
    input.predecessor,
  );
  return { continuedTo: run.runId, deliveries: {}, terminal: false };
}

export async function startTurnStep(
  session: SessionResources,
  submission: AcceptedSubmission,
): Promise<{ readonly runId: string }> {
  "use step";
  const run = await dispatchTurn({ sessionId: session.sessionId, resources: session }, submission);
  return { runId: run.runId };
}
