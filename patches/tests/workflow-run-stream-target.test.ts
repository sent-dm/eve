import { afterEach, describe, expect, it, vi } from "vitest";
import { WORKFLOW_DESERIALIZE, WORKFLOW_SERIALIZE } from "@workflow/serde";
import type { WorkflowRun } from "@workflow/world";
import { bytesToBase64, deriveRunKeyPair } from "#workflow-sdk/sealed-box.js";
import { getWorldLazy } from "#workflow-sdk/runtime/get-world-lazy.js";
import { Run, getRun } from "#workflow-sdk/runtime/run.js";
import {
  STREAM_NAME_SYMBOL,
  STREAM_SERVER_DEPLOYMENT_ID_SYMBOL,
  STREAM_SERVER_PUBLIC_KEY_SYMBOL,
  STREAM_SERVER_RUN_ID_SYMBOL,
} from "#workflow-sdk/symbols.js";

vi.mock("#workflow-sdk/version.js", () => ({ version: "0.0.0-test" }));
vi.mock("#workflow-sdk/runtime/get-world-lazy.js", () => ({ getWorldLazy: vi.fn() }));

type FixtureRun = WorkflowRun & { input: undefined; output: undefined };

const RUN_ID = "wrun_cacheowner";
const MATERIAL = new Uint8Array(32).fill(0x2b);

async function worldFixture({
  encrypted = true,
  material = MATERIAL,
  deploymentId = "dpl_owner",
} = {}) {
  const pair = await deriveRunKeyPair(material);
  let record = {
    runId: RUN_ID,
    status: "running",
    deploymentId,
    workflowName: "key-provider-context",
    ...(encrypted ? { encryptionPublicKey: bytesToBase64(pair.publicKey) } : {}),
  } as FixtureRun;
  const frames = new Map<string, Uint8Array[]>();
  const world = {
    runs: { get: vi.fn(async () => record) },
    streams: {
      write: vi.fn(async (_runId: string, name: string, value: Uint8Array) => {
        const stored = frames.get(name) ?? [];
        stored.push(value.slice());
        frames.set(name, stored);
      }),
      close: vi.fn(async () => {}),
      get: vi.fn(
        async (_runId: string, name: string, startIndex = 0) =>
          new ReadableStream<Uint8Array>({
            start(controller) {
              for (const frame of (frames.get(name) ?? []).slice(startIndex))
                controller.enqueue(frame);
            },
          }),
      ),
      getInfo: vi.fn(async (_runId: string, name: string) => ({
        tailIndex: (frames.get(name)?.length ?? 0) - 1,
        done: false,
      })),
    },
    ...(encrypted
      ? { getEncryptionKeyForRun: vi.fn(async (_run: string | WorkflowRun) => material) }
      : {}),
  };
  vi.mocked(getWorldLazy, { partial: true, deep: true }).mockResolvedValue(world);
  return {
    world,
    frames,
    record: () => record,
    update: (patch: Partial<WorkflowRun>) => {
      record = { ...record, ...patch } as FixtureRun;
    },
  };
}

async function write(run: Run<unknown>, value: string, namespace?: string) {
  const ops: Promise<unknown>[] = [];
  const writable = await run.getWritable<string>({ namespace, ops });
  const writer = writable.getWriter();
  try {
    await writer.write(value);
  } finally {
    writer.releaseLock();
  }
  await Promise.all(ops);
  return { writable, ops };
}

async function read(run: Run<unknown>, startIndex = 0, namespace?: string) {
  const ops: Promise<unknown>[] = [];
  const finished = new Error("Test reader consumed its record");
  const reader = run.getReadable<string>({ startIndex, namespace, ops }).getReader();
  try {
    const result = await reader.read();
    expect(result.done).toBe(false);
    return result.value;
  } finally {
    await reader.cancel(finished);
    reader.releaseLock();
    for (const outcome of await Promise.allSettled(ops)) {
      if (outcome.status === "rejected") expect(outcome.reason).toBe(finished);
    }
  }
}

function prefix(frame: Uint8Array) {
  return new TextDecoder().decode(frame.subarray(4, 8));
}

afterEach(() => vi.clearAllMocks());

describe("Run stream target cache", () => {
  it.each([true, false])(
    "shares read metadata with two subsequent writers (encrypted=%s)",
    async (encrypted) => {
      const { world, frames } = await worldFixture({ encrypted });
      await write(getRun(RUN_ID), "initial");
      world.runs.get.mockClear();
      world.getEncryptionKeyForRun?.mockClear();
      const run = getRun(RUN_ID);
      expect(await read(run)).toBe("initial");
      await write(run, "next");
      await write(run, "namespaced", "events");
      expect(world.runs.get).toHaveBeenCalledTimes(1);
      expect(world.getEncryptionKeyForRun?.mock.calls.length ?? 0).toBe(encrypted ? 1 : 0);
      expect(await read(run, 1)).toBe("next");
      expect(await read(run, 0, "events")).toBe("namespaced");
      expect(world.runs.get).toHaveBeenCalledTimes(1);
      if (encrypted) {
        for (const values of frames.values())
          for (const frame of values) expect(prefix(frame)).toBe("encp");
      }
    },
  );

  it("shares concurrent and repeated writable targets without requesting read keys", async () => {
    const { world, record } = await worldFixture();
    const run = getRun(RUN_ID);
    const [first, second] = await Promise.all([write(run, "one", "one"), write(run, "two", "two")]);
    await write(run, "three", "three");
    expect(world.runs.get).toHaveBeenCalledTimes(1);
    expect(world.getEncryptionKeyForRun).not.toHaveBeenCalled();
    expect(first.ops).not.toBe(second.ops);
    expect(first.ops.length).toBeGreaterThan(0);
    expect(second.ops.length).toBeGreaterThan(0);
    const symbols = (value: WritableStream) => Reflect.get(value, STREAM_NAME_SYMBOL);
    expect(symbols(first.writable)).not.toBe(symbols(second.writable));
    for (const { writable } of [first, second]) {
      expect(Reflect.get(writable, STREAM_SERVER_RUN_ID_SYMBOL)).toBe(RUN_ID);
      expect(Reflect.get(writable, STREAM_SERVER_DEPLOYMENT_ID_SYMBOL)).toBe("dpl_owner");
      expect(Reflect.get(writable, STREAM_SERVER_PUBLIC_KEY_SYMBOL)).toBe(
        record().encryptionPublicKey,
      );
    }
    expect(world.streams.close).not.toHaveBeenCalled();
  });

  it("lets a writer join the metadata request already started by a readable", async () => {
    const { world, record } = await worldFixture();
    await write(getRun(RUN_ID), "initial");
    world.runs.get.mockClear();
    const requested = Promise.withResolvers<void>();
    const metadata = Promise.withResolvers<FixtureRun>();
    world.runs.get.mockImplementationOnce(() => {
      requested.resolve();
      return metadata.promise;
    });
    const run = getRun(RUN_ID);
    const reading = read(run);
    await requested.promise;
    const writing = write(run, "concurrent");
    metadata.resolve(record());
    expect(await reading).toBe("initial");
    await writing;
    expect(world.runs.get).toHaveBeenCalledTimes(1);
  });

  it("retries a failed target lookup without accepting writes or poisoning later calls", async () => {
    const { world } = await worldFixture();
    const failure = new Error("metadata unavailable");
    world.runs.get.mockRejectedValueOnce(failure);
    const run = getRun(RUN_ID);
    await expect(run.getWritable()).rejects.toBe(failure);
    expect(world.streams.write).not.toHaveBeenCalled();
    await write(run, "accepted");
    await write(run, "next");
    expect(world.runs.get).toHaveBeenCalledTimes(2);
  });

  it("retries a failed forwarded key lookup without falling back to plaintext", async () => {
    const { world, frames, update } = await worldFixture();
    update({ encryptionPublicKey: undefined });
    const failure = new Error("key unavailable");
    world.getEncryptionKeyForRun!.mockRejectedValueOnce(failure);
    const run = getRun(RUN_ID);
    await expect(run.getWritable()).rejects.toBe(failure);
    expect(world.streams.write).not.toHaveBeenCalled();
    await write(run, "encrypted");
    await write(run, "still encrypted");
    expect(world.runs.get).toHaveBeenCalledTimes(1);
    expect(world.getEncryptionKeyForRun).toHaveBeenCalledTimes(2);
    for (const values of frames.values())
      for (const frame of values) expect(prefix(frame)).toBe("encr");
  });

  it("keeps status and completion timestamps fresh after caching a stream target", async () => {
    const { world, update } = await worldFixture();
    const run = getRun(RUN_ID);
    await write(run, "cached target");
    expect(await run.status).toBe("running");
    expect(await run.completedAt).toBeUndefined();
    const completedAt = new Date("2026-09-08T17:00:00.000Z");
    update({ status: "completed", completedAt });
    expect(await run.status).toBe("completed");
    expect(await run.completedAt).toEqual(completedAt);
    expect(world.runs.get).toHaveBeenCalledTimes(5);
  });

  it("preserves the full custom World key callback for a read after a write-only target lookup", async () => {
    const { world } = await worldFixture();
    const run = getRun(RUN_ID);
    await write(run, "first");
    expect(world.getEncryptionKeyForRun).not.toHaveBeenCalled();
    expect(await read(run)).toBe("first");
    expect(world.getEncryptionKeyForRun).toHaveBeenCalledWith(
      expect.objectContaining({ workflowName: "key-provider-context" }),
    );
    expect(world.runs.get).toHaveBeenCalledTimes(2);
  });

  it("isolates new and deserialized Run instances in another World with the same run ID", async () => {
    const first = await worldFixture();
    const original = getRun(RUN_ID);
    await write(original, "first world");
    const serialized = Run[WORKFLOW_SERIALIZE](original);
    expect(serialized).toEqual({ runId: RUN_ID, resilientStart: false });
    const second = await worldFixture({
      material: new Uint8Array(32).fill(0x73),
      deploymentId: "dpl_other",
    });
    const restored = Run[WORKFLOW_DESERIALIZE](serialized);
    const written = await write(restored, "second world");
    expect(Reflect.get(written.writable, STREAM_SERVER_DEPLOYMENT_ID_SYMBOL)).toBe("dpl_other");
    expect(Reflect.get(written.writable, STREAM_SERVER_PUBLIC_KEY_SYMBOL)).toBe(
      second.record().encryptionPublicKey,
    );
    expect(second.record().encryptionPublicKey).not.toBe(first.record().encryptionPublicKey);
    expect(await read(restored)).toBe("second world");
    expect(first.world.runs.get).toHaveBeenCalledTimes(1);
    expect(first.world.streams.write).toHaveBeenCalledTimes(1);
    expect(second.world.streams.write).toHaveBeenCalledTimes(1);
  });
});
