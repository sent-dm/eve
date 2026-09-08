import { createHash } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { sessionDirectory } from "#execution/session/directory.js";
import { sessionEvents } from "#execution/session/events.js";
import type { SessionResources } from "#execution/session/resources.js";
import { sessionSnapshots } from "#execution/session/snapshots.js";
import {
  sessionStorageCloseFixtureWorkflow,
  sessionStorageContributorFixtureWorkflow,
  sessionStorageHolderFixtureWorkflow,
  sessionStorageReplayFixtureWorkflow,
  type SessionStorageFixtureCheckpoint,
} from "#internal/testing/session-storage-workflow.js";
import { getWorld, resumeHook, setWorld, start, type Run } from "#internal/workflow/runtime.js";

describe("session storage through native independent workflow contributors", () => {
  let world: Awaited<ReturnType<typeof getWorld>>;
  let previousWorld: typeof world;
  beforeAll(async () => {
    previousWorld = await getWorld();
    world = {
      ...previousWorld,
      getEncryptionKeyForRun: async (run) => {
        const runId = typeof run === "string" ? run : run.runId;
        return new Uint8Array(
          createHash("sha256").update(`session-storage-test:${runId}`).digest(),
        );
      },
    };
    setWorld(world);
  });
  afterAll(() => setWorld(previousWorld));

  it("rejects a nonexistent holder without waiting for a descriptor", async () => {
    await expect(sessionDirectory.resolveHolder("missing-holder")).rejects.toMatchObject({
      name: "WorkflowRunNotFoundError",
    });
  });

  it("preserves holder ownership, encryption, and stream lifetime", async () => {
    let holder: Run<void> | undefined;
    let stopped = false;
    let reader: ReadableStreamDefaultReader | undefined;
    try {
      holder = await start(sessionStorageHolderFixtureWorkflow, { world });
      const holderRunId = holder.runId;
      const resources = await sessionDirectory.resolveHolder(holder.runId);
      expect(resources.holderRunId).toBe(holder.runId);
      expect(resources.control.ownerRunId).toBe(holder.runId);
      expect((await sessionSnapshots.open(resources.snapshots)).latest).toBeUndefined();

      const descriptor = holder
        .getReadable<SessionResources>({ namespace: "eve.session.resources" })
        .getReader();
      try {
        expect(await descriptor.read()).toEqual({ done: false, value: resources });
        expect((await descriptor.read()).done).toBe(true);
      } finally {
        await descriptor.cancel();
        descriptor.releaseLock();
      }

      const first = await start(
        sessionStorageContributorFixtureWorkflow,
        [{ holderRunId: holder.runId, marker: "first" }],
        { world },
      );
      const firstResult = await first.returnValue;
      expect(firstResult).toMatchObject({ writerRunId: first.runId, previousMarker: undefined });
      expect(firstResult.checkpoint).toEqual({ streamId: resources.snapshots.id, index: 1 });
      expect(first.runId).not.toBe(holder.runId);
      expect(await world.streams.list(first.runId)).toEqual([]);
      expect(await holder.status).toBe("running");

      reader = sessionEvents.read(resources.events).getReader();
      expect((await reader.read()).value).toMatchObject({
        type: "message.received",
        data: { message: "first", turnId: first.runId, sequence: 0 },
      });
      const secondEvent = reader.read();
      const ownerStreams = await world.streams.list(holder.runId);
      expect(ownerStreams).toHaveLength(3);
      const openStreams = await Promise.all(
        ownerStreams.map((name) => world.streams.getInfo(holderRunId, name)),
      );
      expect(openStreams.filter((stream) => !stream.done)).toHaveLength(2);

      const second = await start(
        sessionStorageContributorFixtureWorkflow,
        [{ holderRunId: holder.runId, marker: "second" }],
        { world },
      );
      const secondResult = await second.returnValue;
      expect(secondResult).toMatchObject({ writerRunId: second.runId, previousMarker: "first" });
      expect(secondResult.checkpoint).toEqual({ streamId: resources.snapshots.id, index: 2 });
      expect(await world.streams.list(second.runId)).toEqual([]);
      expect((await secondEvent).value).toMatchObject({
        type: "message.received",
        data: { message: "second", turnId: second.runId, sequence: 1 },
      });

      const snapshots = await sessionSnapshots.open<SessionStorageFixtureCheckpoint>(
        resources.snapshots,
      );
      const old = await snapshots.read(firstResult.checkpoint);
      const latest = snapshots.latest;
      expect(old.markers).toEqual(["first"]);
      expect(latest?.ref).toEqual(secondResult.checkpoint);
      expect(latest?.checkpoint.markers).toEqual(["first", "second"]);
      expect(old.state).toEqual(
        new Map([["first", Uint8Array.from([0, 255, ...new TextEncoder().encode("first")])]]),
      );

      const replay = await start(
        sessionStorageReplayFixtureWorkflow,
        [{ holderRunId: holder.runId, checkpoint: secondResult.checkpoint }],
        { world },
      );
      expect(await replay.returnValue).toEqual(secondResult.checkpoint);
      expect(await world.streams.list(replay.runId)).toEqual([]);
      expect(await world.streams.list(holder.runId)).toHaveLength(3);
      const snapshotStream = holder.getReadable({ namespace: "eve.session.snapshots" });
      try {
        expect(await snapshotStream.getTailIndex()).toBe(2);
      } finally {
        await snapshotStream.cancel();
      }

      const ownerRun = await world.runs.get(holder.runId, { resolveData: "none" });
      const contributorRun = await world.runs.get(second.runId, { resolveData: "none" });
      expect(ownerRun.encryptionPublicKey).toEqual(expect.any(String));
      expect(ownerRun.encryptionPublicKey).not.toBe(contributorRun.encryptionPublicKey);
      for (const name of await world.streams.list(holder.runId)) {
        const chunks = await world.streams.getChunks(holder.runId, name, { limit: 1 });
        expect(new TextDecoder().decode(chunks.data[0]!.data.subarray(4, 8))).toBe("encp");
      }

      const end = reader.read();
      const closer = await start(sessionStorageCloseFixtureWorkflow, [holder.runId], { world });
      await closer.returnValue;
      expect((await end).done).toBe(true);
      expect((await sessionSnapshots.open(resources.snapshots)).latest?.ref).toEqual(
        secondResult.checkpoint,
      );
      expect(await holder.status).toBe("running");
      expect(await sessionDirectory.resolveHolder(holder.runId)).toEqual(resources);
      const closedStreams = await Promise.all(
        (await world.streams.list(holder.runId)).map((name) =>
          world.streams.getInfo(holderRunId, name),
        ),
      );
      expect(closedStreams.every((stream) => stream.done)).toBe(true);
      await resumeHook(resources.control.token, { kind: "stop" });
      await holder.returnValue;
      stopped = true;
    } finally {
      if (reader !== undefined) {
        await reader.cancel();
        reader.releaseLock();
      }
      if (!stopped) await holder?.cancel().catch(() => {});
    }
  });
});
