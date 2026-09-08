import { getWritable } from "#compiled/@workflow/core/index.js";
import { getRun } from "#internal/workflow/runtime.js";
import type { InboxAddress } from "#execution/inbox/types.js";
import { encodeStreamLocation } from "#execution/session/stream-location.js";
import { readStreamRecord } from "#execution/session/stream-storage.js";

const OWNER_NAMESPACE = "eve.owner";

/** Publishes the winning owner, including when this start lost its claim. */
export async function publishOwnerStep(address: InboxAddress): Promise<void> {
  "use step";
  const writer = getWritable<InboxAddress>({ namespace: OWNER_NAMESPACE }).getWriter();
  try {
    await writer.write(address);
    await writer.close();
  } finally {
    writer.releaseLock();
  }
}

export async function readStartedOwner(runId: string): Promise<InboxAddress> {
  try {
    return await readStreamRecord<InboxAddress>(
      encodeStreamLocation({ owner: runId, namespace: OWNER_NAMESPACE }),
    );
  } catch (error) {
    const run = getRun(runId);
    const status = await run.status;
    if (status === "failed" || status === "cancelled") await run.returnValue;
    if (status === "completed") {
      throw new Error(`Workflow "${runId}" ended without publishing its owner.`, { cause: error });
    }
    throw error;
  }
}
