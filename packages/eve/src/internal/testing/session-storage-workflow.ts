import { resolveStreamOwner } from "#execution/session/stream-storage.js";
import { createHook, getWorkflowMetadata } from "#compiled/@workflow/core/index.js";
import {
  initializeSessionResources,
  publishSessionDescriptor,
  sessionDirectory,
} from "#execution/session/directory.js";
import { sessionEvents } from "#execution/session/events.js";
import {
  createSessionResources,
  type SessionResources,
  type SnapshotRecordRef,
} from "#execution/session/resources.js";
import { sessionSnapshots } from "#execution/session/snapshots.js";
import {
  createMessageReceivedEvent,
  encodeMessageStreamEvent,
  stampMessageStreamEvent,
} from "#protocol/message.js";

export interface SessionStorageFixtureCheckpoint {
  readonly writeId: string;
  readonly writerRunId: string;
  readonly markers: readonly string[];
  readonly state: Map<string, Uint8Array>;
}

export interface SessionStorageFixtureContribution {
  readonly writerRunId: string;
  readonly previousMarker: string | undefined;
  readonly checkpoint: SnapshotRecordRef;
}

/** The holder publishes references and stays alive independently of all contributors. */
export async function sessionStorageHolderFixtureWorkflow(): Promise<void> {
  "use workflow";
  const runId = getWorkflowMetadata().workflowRunId;
  const control = createHook({ token: `eve:holder:${runId}` });
  try {
    if ((await control.getConflict()) !== null) throw new Error("Fixture holder already exists.");
    await initializeStorageFixtureStep(runId);
    await control;
  } finally {
    control.dispose();
  }
}

async function initializeStorageFixtureStep(runId: string): Promise<void> {
  "use step";
  const owner = await resolveStreamOwner(runId);
  const resources = createSessionResources(runId, "initial", owner);
  await initializeSessionResources(resources);
  await publishSessionDescriptor(owner, resources);
}

/** Only stable identifiers cross the workflow boundary; snapshots stay inside the step. */
export async function sessionStorageContributorFixtureWorkflow(input: {
  readonly holderRunId: string;
  readonly marker: string;
}): Promise<SessionStorageFixtureContribution> {
  "use workflow";
  return await contributeStorageFixtureStep(input);
}

async function contributeStorageFixtureStep(input: {
  readonly holderRunId: string;
  readonly marker: string;
}): Promise<SessionStorageFixtureContribution> {
  "use step";
  const resources = await sessionDirectory.resolveHolder(input.holderRunId);
  const writerRunId = getWorkflowMetadata().workflowRunId;
  const log = await sessionSnapshots.open<SessionStorageFixtureCheckpoint>(resources.snapshots);
  const previous = log.latest;
  const markers = [...(previous?.checkpoint.markers ?? []), input.marker];
  const bytes = Uint8Array.from([0, 255, ...new TextEncoder().encode(input.marker)]);
  const checkpoint = await log.append({
    writeId: writerRunId,
    writerRunId,
    markers,
    state: new Map([[input.marker, bytes]]),
  } satisfies SessionStorageFixtureCheckpoint);
  await sessionEvents.open(resources.events).withWriter(async (writable) => {
    const writer = writable.getWriter();
    try {
      await writer.write(
        encodeMessageStreamEvent(
          stampMessageStreamEvent(
            createMessageReceivedEvent({
              message: input.marker,
              sequence: markers.length - 1,
              turnId: writerRunId,
            }),
          ),
        ),
      );
    } finally {
      writer.releaseLock();
    }
  });
  return { checkpoint, previousMarker: previous?.checkpoint.markers.at(-1), writerRunId };
}
contributeStorageFixtureStep.maxRetries = 0;

export async function sessionStorageReplayFixtureWorkflow(input: {
  readonly holderRunId: string;
  readonly checkpoint: SnapshotRecordRef;
}): Promise<SnapshotRecordRef> {
  "use workflow";
  return replayStorageFixtureStep(input);
}

async function replayStorageFixtureStep(input: {
  readonly holderRunId: string;
  readonly checkpoint: SnapshotRecordRef;
}): Promise<SnapshotRecordRef> {
  "use step";
  const resources = await sessionDirectory.resolveHolder(input.holderRunId);
  const log = await sessionSnapshots.open<SessionStorageFixtureCheckpoint>(resources.snapshots);
  return log.append(await log.read(input.checkpoint));
}
replayStorageFixtureStep.maxRetries = 0;

export async function sessionStorageCloseFixtureWorkflow(holderRunId: string): Promise<void> {
  "use workflow";
  await closeStorageFixtureStep(holderRunId);
}

async function closeStorageFixtureStep(holderRunId: string): Promise<void> {
  "use step";
  const resources: SessionResources = await sessionDirectory.resolveHolder(holderRunId);
  await sessionEvents.open(resources.events).close();
  await sessionSnapshots.close(resources.snapshots);
}
