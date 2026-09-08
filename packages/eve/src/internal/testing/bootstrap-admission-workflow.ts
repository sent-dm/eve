import { createHook, getWorkflowMetadata, getWritable } from "#compiled/@workflow/core/index.js";
import { startTurnStep } from "#execution/session/dispatch.js";
import { initializeHolderStep } from "#execution/session/holding-steps.js";
import {
  isSessionStorageUnavailable,
  SessionStorageUnavailableError,
} from "#execution/session/storage-error.js";
import type { AcceptedSubmission } from "#execution/turn/types.js";

/** Publishes the real bootstrap seed while a gate delays its original candidate. */
export async function bootstrapAdmissionHolderWorkflow(
  firstTurn: AcceptedSubmission,
): Promise<void> {
  "use workflow";
  const runId = getWorkflowMetadata().workflowRunId;
  const control = createHook({ token: `eve:holder:${runId}` });
  const gate = createHook({ token: `test:bootstrap:${runId}` });
  try {
    const claims = await Promise.all([control.getConflict(), gate.getConflict()]);
    if (claims.some((claim) => claim !== null)) throw new Error("Fixture hook already exists.");
    const session = await initializeHolderStep(runId, firstTurn);
    await gate;
    const candidate = await startTurnStep(session, firstTurn);
    await publishBootstrapCandidateStep(candidate.runId);
    await control;
  } finally {
    gate.dispose();
    control.dispose();
  }
}

async function publishBootstrapCandidateStep(runId: string): Promise<void> {
  "use step";
  const writer = getWritable<string>({ namespace: "test.bootstrap.candidate" }).getWriter();
  try {
    await writer.write(runId);
    await writer.close();
  } finally {
    writer.releaseLock();
  }
}

export async function storageErrorRoundTripWorkflow(): Promise<boolean> {
  "use workflow";
  try {
    await unavailableStorageFixtureStep();
    return false;
  } catch (error) {
    return isSessionStorageUnavailable(error);
  }
}

async function unavailableStorageFixtureStep(): Promise<void> {
  "use step";
  throw new SessionStorageUnavailableError(new Error("Unavailable fixture storage."));
}
unavailableStorageFixtureStep.maxRetries = 0;
