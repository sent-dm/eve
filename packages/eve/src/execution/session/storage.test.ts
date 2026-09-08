import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  initializeSessionResources,
  publishSessionDescriptor,
  sessionDirectory,
  resolveSessionTarget,
} from "#execution/session/directory.js";
import { sessionEvents } from "#execution/session/events.js";
import { createSessionResources, type SnapshotRecordRef } from "#execution/session/resources.js";
import { createStreamStorageScope, resolveStreamOwner } from "#execution/session/stream-storage.js";
import { decodeStreamLocation, encodeStreamLocation } from "#execution/session/stream-location.js";
import { sessionSnapshots } from "#execution/session/snapshots.js";
import { createSessionStartedEvent, stampMessageStreamEvent } from "#protocol/message.js";

const runtime = vi.hoisted(() => ({
  getRun: vi.fn(),
  getWorld: vi.fn(),
  Run: { fromStreamReference: vi.fn() },
}));
vi.mock("#internal/workflow/runtime.js", () => runtime);

interface StoredStream {
  chunks: unknown[];
  closed: boolean;
}

const streams = new Map<string, StoredStream>();
let failWrite: string | undefined;
let failAfterWrite = false;
let pauseFlush: Promise<void> | undefined;
const streamFlushes = new Map<string, Promise<void>>();
let reads = 0;
let tailReads = 0;
let writes = 0;
let closes = 0;
let cancellations = 0;
const nativeWritables: WritableStream[] = [];
let nativeWriterAcquisitions = 0;
let nativeWriterReleases = 0;

function createResources(runId: string, initialEventId: string) {
  return createSessionResources(runId, initialEventId, { runId, deploymentId: "owner-deployment" });
}

function storageKey(id: string): string {
  const { owner, namespace } = decodeStreamLocation(id);
  return streamKey(typeof owner === "string" ? owner : owner.runId, namespace);
}

function streamKey(runId: string, namespace: string | undefined): string {
  return JSON.stringify([runId, namespace ?? null]);
}

function stored(id: string): StoredStream {
  const key = storageKey(id);
  let stream = streams.get(key);
  if (stream === undefined) {
    stream = { chunks: [], closed: false };
    streams.set(key, stream);
  }
  return stream;
}

beforeEach(() => {
  runtime.getRun.mockClear();
  runtime.Run.fromStreamReference.mockClear();
  streams.clear();
  failWrite = undefined;
  failAfterWrite = false;
  pauseFlush = undefined;
  streamFlushes.clear();
  reads = 0;
  tailReads = 0;
  writes = 0;
  closes = 0;
  cancellations = 0;
  nativeWritables.length = 0;
  nativeWriterAcquisitions = 0;
  nativeWriterReleases = 0;
  runtime.getWorld.mockResolvedValue({});
  const streamAccess = (runId: string) => ({
    status: Promise.resolve("running"),
    getReadable: (options: { namespace?: string; startIndex?: number } = {}) => {
      const source = stored(streamKey(runId, options.namespace));
      let index = options.startIndex ?? 0;
      if (index < 0) index = Math.max(0, source.chunks.length + index);
      const stream = new ReadableStream<unknown>(
        {
          pull(controller) {
            reads++;
            if (index < source.chunks.length)
              controller.enqueue(structuredClone(source.chunks[index++]));
            else if (source.closed) controller.close();
          },
          cancel() {
            cancellations++;
          },
        },
        { highWaterMark: 0 },
      );
      return Object.assign(stream, {
        getTailIndex: async () => {
          tailReads++;
          return source.chunks.length - 1;
        },
      });
    },
    getWritable: async (options: { namespace?: string; ops: Promise<unknown>[] }) => {
      const key = streamKey(runId, options.namespace);
      const source = stored(key);
      const pending: unknown[] = [];
      let closing = false;
      let finish!: () => void;
      const released = new Promise<void>((resolve) => {
        finish = resolve;
      });
      const flushed = released.then(async () => {
        await pauseFlush;
        await streamFlushes.get(key);
        if (failWrite !== undefined && key.includes(failWrite))
          throw new Error("Storage unavailable");
        if (source.closed && pending.length > 0) throw new Error("Stream is closed");
        source.chunks.push(...pending);
        if (closing) source.closed = true;
        if (failAfterWrite) throw new Error("Write acknowledgement unavailable");
      });
      options.ops.push(flushed);
      const writable = new WritableStream({
        write(value) {
          writes++;
          pending.push(structuredClone(value));
        },
        close() {
          closes++;
          closing = true;
        },
      });
      nativeWritables.push(writable);
      const getWriter = writable.getWriter.bind(writable);
      writable.getWriter = () => {
        nativeWriterAcquisitions++;
        const writer = getWriter();
        const release = writer.releaseLock.bind(writer);
        writer.releaseLock = () => {
          release();
          nativeWriterReleases++;
          finish();
        };
        return writer;
      };
      return writable;
    },
  });
  runtime.getRun.mockImplementation((runId: string) => ({
    ...streamAccess(runId),
    getStreamReference: async () => ({ runId, deploymentId: "owner-deployment" }),
  }));
  runtime.Run.fromStreamReference.mockImplementation((owner: { runId: string }) =>
    streamAccess(owner.runId),
  );
});

afterEach(() => {
  vi.useRealTimers();
});

describe("session directory", () => {
  it("propagates descriptor read failures without an existence preflight", async () => {
    const failure = new Error("Descriptor unavailable");
    runtime.getRun.mockReturnValueOnce({
      getReadable: () => new ReadableStream({ start: (controller) => controller.error(failure) }),
    });
    await expect(sessionDirectory.resolveHolder("missing")).rejects.toBe(failure);
    expect(reads).toBe(0);
    expect(streams.size).toBe(0);
  });

  it("rejects a redirected holder before accessing another session's snapshots", async () => {
    const resources = createResources("canonical", "first");
    await publishSessionDescriptor(
      { runId: "redirect", deploymentId: "owner-deployment" },
      resources,
    );
    await expect(
      resolveSessionTarget({ sessionId: "redirect" }, createStreamStorageScope()),
    ).rejects.toThrow("do not match the claimed session");
    expect(streams.has(storageKey(resources.snapshots.id))).toBe(false);
  });

  it("uses supplied bootstrap resources without reading an unpublished descriptor", async () => {
    const resources = createResources("holder", "first");
    expect(
      await resolveSessionTarget({ sessionId: "holder", resources }, createStreamStorageScope()),
    ).toBe(resources);
    expect(runtime.getRun).not.toHaveBeenCalled();
  });

  it("resolves the canonical descriptor after duplicate holder bootstrap", async () => {
    const resources = createResources("winner", "first");
    await initializeSessionResources(resources);
    await publishSessionDescriptor(
      { runId: resources.holderRunId, deploymentId: "owner-deployment" },
      resources,
    );
    await publishSessionDescriptor({ runId: "loser", deploymentId: "owner-deployment" }, resources);

    expect(await sessionDirectory.resolveHolder("loser")).toEqual(resources);
    expect(await sessionDirectory.resolveSession(resources.sessionId)).toEqual(resources);
    expect(stored(streamKey("loser", "eve.session.resources")).closed).toBe(true);
    expect(streams.has(streamKey("loser", "eve.session.snapshots"))).toBe(false);
  });

  it("caches only successful immutable descriptors within one storage scope", async () => {
    const resources = createResources("holder", "first");
    await initializeSessionResources(resources);
    await publishSessionDescriptor(
      { runId: resources.holderRunId, deploymentId: "owner-deployment" },
      resources,
    );
    const descriptor = await sessionDirectory.resolveHolder("holder");
    const firstReads = reads;
    expect(await sessionDirectory.resolveHolder("holder")).toBe(descriptor);
    expect(reads).toBe(firstReads);
    expect(Object.isFrozen(descriptor)).toBe(true);
    expect(Object.isFrozen(descriptor.events)).toBe(true);

    runtime.getWorld.mockResolvedValue({});
    expect(await sessionDirectory.resolveHolder("holder")).not.toBe(descriptor);
    expect(reads).toBeGreaterThan(firstReads);
  });

  it("publishes readiness once and rejects a changed resource descriptor on retry", async () => {
    const resources = createResources("holder", "first");
    await initializeSessionResources(resources);
    expect(streams.has(streamKey("holder", "eve.session.resources"))).toBe(false);
    runtime.getRun.mockClear();
    await publishSessionDescriptor(
      { runId: "holder", deploymentId: "owner-deployment" },
      resources,
    );
    await publishSessionDescriptor(
      { runId: "holder", deploymentId: "owner-deployment" },
      resources,
    );
    expect(runtime.getRun).not.toHaveBeenCalled();
    expect(stored(streamKey("holder", "eve.session.resources")).chunks).toHaveLength(1);
    await expect(
      publishSessionDescriptor(
        { runId: "holder", deploymentId: "owner-deployment" },
        { ...resources, initialEventId: "changed" },
      ),
    ).rejects.toThrow("different contents");
  });
});

describe("step storage scopes", () => {
  it("resolves public ownership once without leaking other SDK reference fields", async () => {
    const issued = vi.fn(async () => ({
      runId: "owner",
      deploymentId: "deployment",
      encryptionPublicKey: "public-key",
      namespace: "ignored-default",
      internal: "not part of eve routing",
    }));
    runtime.getRun.mockReturnValueOnce({ getStreamReference: issued });
    expect(await resolveStreamOwner("owner")).toEqual({
      runId: "owner",
      deploymentId: "deployment",
      encryptionPublicKey: "public-key",
    });
    expect(runtime.getRun).toHaveBeenCalledExactlyOnceWith("owner");
    expect(issued).toHaveBeenCalledExactlyOnceWith();
    expect(runtime.Run.fromStreamReference).not.toHaveBeenCalled();
  });

  it("reuses a descriptor owner for resolved namespaces without replacing its read-key cache", async () => {
    const resources = createResources("holder", "first");
    await publishSessionDescriptor(
      { runId: "holder", deploymentId: "owner-deployment" },
      resources,
    );
    runtime.getRun.mockClear();
    runtime.Run.fromStreamReference.mockClear();
    const scope = createStreamStorageScope();
    await scope
      .open(encodeStreamLocation({ owner: "holder", namespace: "eve.session.resources" }))
      .readRecord();
    await scope.open(resources.events.id).append(["event"]);
    await scope.open(resources.snapshots.id).append(["snapshot"]);
    expect(runtime.getRun).toHaveBeenCalledExactlyOnceWith("holder");
    expect(runtime.Run.fromStreamReference).not.toHaveBeenCalled();
  });

  it("shares owner resolution between snapshot and event streams only within each step", async () => {
    const resources = createResources("holder", "initial");
    await sessionSnapshots.initialize(resources.snapshots);
    runtime.Run.fromStreamReference.mockClear();
    for (const phase of ["execute", "finalize"]) {
      const scope = createStreamStorageScope();
      const snapshots = await sessionSnapshots.open(resources.snapshots, scope);
      const events = sessionEvents.open(resources.events, scope);
      await snapshots.append({ writeId: `${phase}:entered` });
      await events.withWriter(async (writable) => {
        const writer = writable.getWriter();
        try {
          await writer.write(new Uint8Array([1]));
        } finally {
          writer.releaseLock();
        }
      });
      await snapshots.append({ writeId: phase });
    }
    expect(runtime.getRun).not.toHaveBeenCalled();
    expect(runtime.Run.fromStreamReference.mock.calls).toEqual([
      [{ runId: "holder", deploymentId: "owner-deployment" }],
      [{ runId: "holder", deploymentId: "owner-deployment" }],
    ]);
    expect(runtime.Run.fromStreamReference.mock.results[0]?.value).not.toBe(
      runtime.Run.fromStreamReference.mock.results[1]?.value,
    );
    expect(stored(resources.events.id).chunks).toHaveLength(2);
    expect(stored(resources.snapshots.id).chunks).toHaveLength(5);
  });

  it("resolves independently owned streams without coupling them to the logical holder", async () => {
    const resources = {
      ...createResources("holder", "initial"),
      events: createResources("event-owner", "initial").events,
      snapshots: createResources("snapshot-owner", "initial").snapshots,
    };
    const scope = createStreamStorageScope();
    await sessionSnapshots.initialize(resources.snapshots, scope);
    const snapshots = await sessionSnapshots.open(resources.snapshots, scope);
    await snapshots.append({ writeId: "committed" });
    await sessionEvents.open(resources.events, scope).withWriter(async (writable) => {
      const writer = writable.getWriter();
      try {
        await writer.write(new Uint8Array([2]));
      } finally {
        writer.releaseLock();
      }
    });
    expect(runtime.getRun).not.toHaveBeenCalled();
    expect(runtime.Run.fromStreamReference.mock.calls).toEqual([
      [{ runId: "snapshot-owner", deploymentId: "owner-deployment" }],
      [{ runId: "event-owner", deploymentId: "owner-deployment" }],
    ]);
    expect([...streams.keys()]).toEqual([
      storageKey(resources.snapshots.id),
      storageKey(resources.events.id),
    ]);
    expect(snapshots.latest?.ref.streamId).toBe(resources.snapshots.id);
  });

  it("keeps independent flush and close boundaries when streams share a Run", async () => {
    const resources = createResources("holder", "initial");
    const scope = createStreamStorageScope();
    const snapshots = scope.open(resources.snapshots.id);
    const events = sessionEvents.open(resources.events, scope);
    const flushing = Promise.withResolvers<void>();
    streamFlushes.set(storageKey(resources.events.id), flushing.promise);
    let eventDurable = false;
    const pending = events
      .withWriter(async (writable) => {
        const writer = writable.getWriter();
        try {
          await writer.write(new Uint8Array([3]));
        } finally {
          writer.releaseLock();
        }
      })
      .then(() => {
        eventDurable = true;
      });
    await snapshots.append([{ marker: "independent" }]);
    expect(eventDurable).toBe(false);
    expect(stored(resources.snapshots.id).chunks).toEqual([{ marker: "independent" }]);
    expect(stored(resources.events.id).chunks).toHaveLength(0);
    flushing.resolve();
    await pending;
    expect(eventDurable).toBe(true);
    await events.close();
    expect(stored(resources.events.id).closed).toBe(true);
    expect(stored(resources.snapshots.id).closed).toBe(false);
    await snapshots.append([{ marker: "still open" }]);
    expect(runtime.getRun).not.toHaveBeenCalled();
    expect(runtime.Run.fromStreamReference).toHaveBeenCalledExactlyOnceWith({
      runId: "holder",
      deploymentId: "owner-deployment",
    });
  });
});

describe("session snapshots", () => {
  it("uses one head read per step and four appends for a warm execute/finalize transaction", async () => {
    type Checkpoint = {
      readonly writeId: string;
      readonly history?: string[];
      readonly previous?: SnapshotRecordRef;
    };
    const { snapshots } = createResources("holder", "first");
    await sessionSnapshots.initialize(snapshots);
    const seed = await sessionSnapshots.open<Checkpoint>(snapshots);
    const previous = await seed.append({ writeId: "previous", history: ["before"] });
    reads = tailReads = writes = closes = 0;
    nativeWriterAcquisitions = 0;
    runtime.getRun.mockClear();
    runtime.Run.fromStreamReference.mockClear();

    const execute = await sessionSnapshots.open<Checkpoint>(snapshots);
    expect(await execute.read(previous)).toEqual({ writeId: "previous", history: ["before"] });
    await execute.append({ writeId: "execute:entered", previous });
    const executed = await execute.append({ writeId: "execute", history: ["before", "after"] });
    const finalize = await sessionSnapshots.open<Checkpoint>(snapshots);
    expect((await finalize.read(executed)).writeId).toBe("execute");
    await finalize.append({ writeId: "finalize:entered", previous: executed });
    await finalize.append({ writeId: "finalize", history: ["before", "after"] });

    expect({
      tailReads,
      reads,
      writes,
      closes,
      writers: nativeWriterAcquisitions,
      runs: runtime.getRun.mock.calls.length,
      resolvedOwners: runtime.Run.fromStreamReference.mock.calls.length,
    }).toEqual({
      tailReads: 0,
      reads: 2,
      writes: 4,
      closes: 0,
      writers: 4,
      runs: 0,
      resolvedOwners: 2,
    });
  });

  it("returns an empty initialized snapshot without waiting for a future writer", async () => {
    const { snapshots } = createResources("holder", "first");
    await sessionSnapshots.initialize(snapshots);
    await sessionSnapshots.initialize(snapshots);
    expect((await sessionSnapshots.open(snapshots)).latest).toBeUndefined();
    expect(stored(snapshots.id).chunks).toHaveLength(1);
    expect(cancellations).toBeGreaterThan(0);
  });

  it("keeps the immutable seed at index zero when holder initialization retries after a turn", async () => {
    const { snapshots } = createResources("holder", "first");
    const seed = { writeId: "first", message: "seed" };
    await sessionSnapshots.initialize(snapshots, undefined, seed);
    const log = await sessionSnapshots.open<typeof seed>(snapshots);
    expect(log.latest).toEqual({ ref: { streamId: snapshots.id, index: 0 }, checkpoint: seed });
    const committed = await log.append({ writeId: "turn", message: "settled" });
    await sessionSnapshots.initialize(snapshots, undefined, seed);
    const reopened = await sessionSnapshots.open<typeof seed>(snapshots);
    expect(reopened.latest?.ref).toEqual(committed);
    expect(await reopened.read({ streamId: snapshots.id, index: 0 })).toEqual(seed);
    expect(stored(snapshots.id).chunks).toHaveLength(2);
  });

  it.each([
    { kind: "initialized", index: 1 },
    { kind: "record", index: 0, checkpoint: { writeId: "invalid" } },
    { kind: "record", index: 1.5, checkpoint: { writeId: "invalid" } },
    { kind: "unknown", index: 1 },
  ])("rejects an invalid snapshot tail envelope: $kind at $index", async (entry) => {
    const { snapshots } = createResources("holder", "first");
    stored(snapshots.id).chunks.push(entry);
    await expect(sessionSnapshots.open(snapshots)).rejects.toThrow("invalid record index");
  });

  it("rejects a historical record whose embedded index does not match its reference", async () => {
    const { snapshots } = createResources("holder", "first");
    await sessionSnapshots.initialize(snapshots);
    const log = await sessionSnapshots.open(snapshots);
    const first = await log.append({ writeId: "first" });
    await log.append({ writeId: "second" });
    stored(snapshots.id).chunks[1] = { kind: "record", index: 2, checkpoint: { writeId: "wrong" } };
    await expect(log.read(first)).rejects.toThrow("does not match its reference");
  });

  it("keeps exact records in one open stream as later checkpoints become latest", async () => {
    const { snapshots } = createResources("holder", "first");
    await sessionSnapshots.initialize(snapshots);
    const log = await sessionSnapshots.open<{
      writeId: string;
      history: string[];
      state?: Map<string, number>;
    }>(snapshots);
    const initial = { writeId: "first:commit", history: ["one"], state: new Map([["count", 1]]) };
    const first = await log.append(initial);
    const next = { writeId: "second:commit", history: ["one", "two"] };
    const second = await log.append(next);

    expect(first).toEqual({ streamId: snapshots.id, index: 1 });
    expect(second).toEqual({ streamId: snapshots.id, index: 2 });
    expect(await log.read(first)).toEqual(initial);
    expect((await sessionSnapshots.open(snapshots)).latest?.ref).toEqual(second);
    expect(await log.append(next)).toEqual(second);
    expect(stored(snapshots.id).chunks).toHaveLength(3);
    expect(stored(snapshots.id).closed).toBe(false);
    expect([...streams.keys()]).toEqual([storageKey(snapshots.id)]);
    await expect(log.append({ ...next, history: ["changed"] })).rejects.toThrow("different state");
    await sessionSnapshots.close(snapshots);
    expect(stored(snapshots.id).closed).toBe(true);
  });

  it("compares retries against the durable value even when the caller mutates its checkpoint", async () => {
    const { snapshots } = createResources("holder", "first");
    await sessionSnapshots.initialize(snapshots);
    const log = await sessionSnapshots.open<{ writeId: string; history: string[] }>(snapshots);
    const checkpoint = { writeId: "commit", history: ["accepted"] };
    await log.append(checkpoint);
    checkpoint.history.push("uncommitted");
    await expect(log.append(checkpoint)).rejects.toThrow("different state");
    expect(
      (await sessionSnapshots.open<{ writeId: string; history: string[] }>(snapshots)).latest
        ?.checkpoint.history,
    ).toEqual(["accepted"]);
  });

  it("recovers a committed write after its acknowledgement fails without appending twice", async () => {
    const { snapshots } = createResources("holder", "first");
    await sessionSnapshots.initialize(snapshots);
    const log = await sessionSnapshots.open<{ writeId: string; history: string[] }>(snapshots);
    const checkpoint = { writeId: "commit", history: ["accepted"] };
    failAfterWrite = true;
    await expect(log.append(checkpoint)).rejects.toThrow("Write acknowledgement unavailable");
    await expect(log.append(checkpoint)).rejects.toThrow("reopen the log");
    expect(() => log.latest).toThrow("reopen the log");
    failAfterWrite = false;
    const recovered = await sessionSnapshots.open<typeof checkpoint>(snapshots);
    expect(recovered.latest?.checkpoint).toEqual(checkpoint);
    expect(await recovered.append(checkpoint)).toEqual({ streamId: snapshots.id, index: 1 });
    expect(stored(snapshots.id).chunks).toHaveLength(2);
  });

  it("keeps an entered marker visible when the following checkpoint fails to commit", async () => {
    type Entry =
      | { writeId: string; history: string[] }
      | { writeId: string; previous: SnapshotRecordRef };
    const { snapshots } = createResources("holder", "first");
    await sessionSnapshots.initialize(snapshots);
    const log = await sessionSnapshots.open<Entry>(snapshots);
    const before = await log.append({ writeId: "before", history: [] });
    const marker = { writeId: "execute:entered", previous: before };
    await log.append(marker);
    failWrite = "eve.session.snapshots";
    await expect(log.append({ writeId: "execute", history: ["accepted"] })).rejects.toThrow(
      "Storage unavailable",
    );
    await expect(log.read(before)).rejects.toThrow("reopen the log");
    failWrite = undefined;
    const reopened = await sessionSnapshots.open<Entry>(snapshots);
    expect(reopened.latest?.checkpoint).toEqual(marker);
    expect(await reopened.read(before)).toEqual({ writeId: "before", history: [] });
  });

  it("rejects concurrent append attempts while the previous write is awaiting durability", async () => {
    const { snapshots } = createResources("holder", "first");
    await sessionSnapshots.initialize(snapshots);
    const log = await sessionSnapshots.open(snapshots);
    const flush = Promise.withResolvers<void>();
    pauseFlush = flush.promise;
    const first = log.append({ writeId: "first" });
    await expect(log.append({ writeId: "second" })).rejects.toThrow("must be sequential");
    flush.resolve();
    expect(await first).toEqual({ streamId: snapshots.id, index: 1 });
    expect(await log.append({ writeId: "second" })).toEqual({ streamId: snapshots.id, index: 2 });
  });

  it("reads historical records by exact index with bounded work", async () => {
    const { snapshots } = createResources("holder", "first");
    await sessionSnapshots.initialize(snapshots);
    const log = await sessionSnapshots.open(snapshots);
    const first = await log.append({ writeId: "first" });
    for (let index = 0; index < 100; index++) await log.append({ writeId: `later-${index}` });
    reads = tailReads = 0;
    expect(await log.read(first)).toEqual({ writeId: "first" });
    expect({ reads, tailReads }).toEqual({ reads: 1, tailReads: 0 });
    await expect(log.read({ ...first, index: 0 })).rejects.toThrow(
      "Session snapshot record does not exist",
    );
    await expect(log.read({ ...first, index: 102 })).rejects.toThrow("outside this log");
    const other = createResources("other", "first").snapshots;
    await expect(log.read({ ...first, streamId: other.id })).rejects.toThrow("outside this log");
  });
});

describe("session event writes", () => {
  it("holds one native writer through delayed acquisition and borrower lock gaps", async () => {
    const { events } = createResources("holder", "first");
    const entered = Promise.withResolvers<void>();
    const start = Promise.withResolvers<void>();
    const between = Promise.withResolvers<void>();
    const resume = Promise.withResolvers<void>();
    const returned = Promise.withResolvers<void>();
    const flush = Promise.withResolvers<void>();
    pauseFlush = flush.promise;
    let completed = false;
    const pending = sessionEvents
      .open(events)
      .withWriter(async (writable) => {
        entered.resolve();
        await start.promise;
        const first = writable.getWriter();
        await first.write(new Uint8Array([1]));
        first.releaseLock();
        between.resolve();
        await resume.promise;
        const second = writable.getWriter();
        await second.write(new Uint8Array([2]));
        second.releaseLock();
        returned.resolve();
      })
      .then(() => {
        completed = true;
      });

    await entered.promise;
    expect(nativeWritables[0]?.locked).toBe(true);
    expect(nativeWriterAcquisitions).toBe(1);
    start.resolve();
    await between.promise;
    expect(nativeWritables[0]?.locked).toBe(true);
    expect(nativeWriterReleases).toBe(0);
    resume.resolve();
    await returned.promise;
    await Promise.resolve();
    expect(nativeWriterAcquisitions).toBe(1);
    expect(nativeWriterReleases).toBe(1);
    expect(nativeWritables[0]?.locked).toBe(false);
    expect(completed).toBe(false);
    expect(stored(events.id).chunks).toEqual([]);
    flush.resolve();
    await pending;
    expect(stored(events.id)).toEqual({
      chunks: [new Uint8Array([1]), new Uint8Array([2])],
      closed: false,
    });
  });

  it("waits for released writer operations to persist without closing the shared stream", async () => {
    const { events } = createResources("holder", "first");
    let flush!: () => void;
    pauseFlush = new Promise<void>((resolve) => {
      flush = resolve;
    });
    const event = stampMessageStreamEvent(createSessionStartedEvent());
    let completed = false;
    const pending = sessionEvents
      .open(events)
      .append([event])
      .then(() => {
        completed = true;
      });
    await Promise.resolve();
    expect(completed).toBe(false);
    expect(stored(events.id).chunks).toHaveLength(0);
    flush();
    await pending;
    expect(stored(events.id).closed).toBe(false);
    const reader = sessionEvents.read(events).getReader();
    expect((await reader.read()).value).toEqual(event);
    await reader.cancel();
    reader.releaseLock();
    expect(await sessionEvents.tailIndex(events)).toBe(0);
  });

  it("drains durable writes before preserving a callback failure", async () => {
    const { events } = createResources("holder", "first");
    let flush!: () => void;
    pauseFlush = new Promise<void>((resolve) => {
      flush = resolve;
    });
    const failure = new Error("Callback failed");
    const pending = sessionEvents.open(events).withWriter(async (writable) => {
      const writer = writable.getWriter();
      await writer.write(new Uint8Array([1]));
      writer.releaseLock();
      throw failure;
    });
    const assertion = expect(pending).rejects.toBe(failure);
    expect(stored(events.id).chunks).toHaveLength(0);
    flush();
    await assertion;
    expect(stored(events.id).chunks).toEqual([new Uint8Array([1])]);
  });

  it("reports both callback and durability failures without masking either", async () => {
    const { events } = createResources("holder", "first");
    failWrite = "holder";
    const failure = new Error("Callback failed");
    const pending = sessionEvents.open(events).withWriter(async (writable) => {
      const writer = writable.getWriter();
      await writer.write(new Uint8Array([1]));
      writer.releaseLock();
      throw failure;
    });
    await expect(pending).rejects.toMatchObject({
      errors: [failure, expect.objectContaining({ message: "Storage unavailable" })],
    });
  });

  it("releases the native writer and flushes even when a failing callback keeps its borrower locked", async () => {
    const { events } = createResources("holder", "first");
    const failure = new Error("Callback failed");
    const pending = sessionEvents.open(events).withWriter(async (writable) => {
      const writer = writable.getWriter();
      await writer.write(new Uint8Array([1]));
      throw failure;
    });
    await expect(pending).rejects.toMatchObject({
      errors: [failure, expect.objectContaining({ message: expect.stringContaining("released") })],
    });
    expect(nativeWriterReleases).toBe(1);
    expect(nativeWritables[0]?.locked).toBe(false);
    expect(stored(events.id)).toEqual({ chunks: [new Uint8Array([1])], closed: false });
  });
});
