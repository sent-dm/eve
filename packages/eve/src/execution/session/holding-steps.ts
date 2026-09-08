import { createSessionResources } from "#execution/session/resources.js";
import {
  initializeSessionResources,
  publishSessionDescriptor,
  sessionDirectory,
} from "#execution/session/directory.js";
import type { SessionResources } from "#execution/session/resources.js";
import { dispatchTurn } from "#execution/session/dispatch.js";
import type { AcceptedSubmission } from "#execution/turn/types.js";

export async function initializeHolderStep(
  runId: string,
  firstTurn: AcceptedSubmission,
): Promise<SessionResources> {
  "use step";
  const resources = createSessionResources(runId, firstTurn.eventId);
  await initializeSessionResources(resources, firstTurn);
  await publishSessionDescriptor(runId, resources);
  return resources;
}

export async function redirectHolderStep(
  runId: string,
  ownerRunId: string,
  submission: AcceptedSubmission,
): Promise<void> {
  "use step";
  const resources = await sessionDirectory.resolveHolder(ownerRunId);
  await dispatchTurn({ sessionId: resources.sessionId, resources }, submission);
  await publishSessionDescriptor(runId, resources);
}
