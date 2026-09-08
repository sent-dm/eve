import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { getWorldLazy } from "#workflow-sdk/runtime/get-world-lazy.js";
import { WorkflowServerWritableStream } from "#workflow-sdk/serialization.js";
import { STREAM_DRAIN_SYMBOL } from "#workflow-sdk/symbols.js";

vi.mock("#workflow-sdk/runtime/get-world-lazy.js", () => ({ getWorldLazy: vi.fn() }));

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubEnv("WORKFLOW_STREAM_FLUSH_INTERVAL_MS", undefined);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.clearAllMocks();
});

function fixture(streamFlushIntervalMs?: number) {
  const chunks: Uint8Array[] = [];
  const append = (values: readonly Uint8Array[]) => {
    chunks.push(...values.map((value) => value.slice()));
  };
  const world = {
    streamFlushIntervalMs,
    streams: {
      write: vi.fn(async (_runId: string, _name: string, value: Uint8Array) => append([value])),
      writeMulti: vi.fn(async (_runId: string, _name: string, values: Uint8Array[]) =>
        append(values),
      ),
      close: vi.fn(async () => {}),
    },
  };
  vi.mocked(getWorldLazy, { partial: true, deep: true }).mockResolvedValue(world);
  const stream = new WorkflowServerWritableStream("wrun_group_commit", "events");
  const writer = stream.getWriter();
  return {
    world,
    writer,
    append,
    values: () => chunks.map((chunk) => [...chunk]),
    write: (value: number) => writer.write(Uint8Array.of(value)),
    drain: Reflect.get(stream, STREAM_DRAIN_SYMBOL) as () => Promise<void>,
  };
}

it("coalesces a same-tick burst by default while preserving each indexed chunk", async () => {
  const f = fixture();
  try {
    await f.write(1);
    await f.write(2);
    await f.write(3);
    expect(f.world.streams.write).not.toHaveBeenCalled();
    expect(f.world.streams.writeMulti).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await f.drain();
    expect(f.world.streams.write).not.toHaveBeenCalled();
    expect(f.world.streams.writeMulti).toHaveBeenCalledTimes(1);
    expect(f.values()).toEqual([[1], [2], [3]]);
  } finally {
    f.writer.releaseLock();
  }
});

it.each(["world", "environment"])(
  "keeps an explicit %s zero-delay override immediate",
  async (source) => {
    if (source === "environment") vi.stubEnv("WORKFLOW_STREAM_FLUSH_INTERVAL_MS", "0");
    const f = fixture(source === "world" ? 0 : 20);
    try {
      await f.write(1);
      await vi.advanceTimersByTimeAsync(0);
      expect(f.world.streams.write).toHaveBeenCalledTimes(1);
      expect(f.world.streams.writeMulti).not.toHaveBeenCalled();
      expect(f.values()).toEqual([[1]]);
      await f.drain();
    } finally {
      f.writer.releaseLock();
    }
  },
);

it("keeps the durability barrier pending through an in-flight group and its accepted tail", async () => {
  const f = fixture();
  const first = Promise.withResolvers<void>();
  const last = Promise.withResolvers<void>();
  const tailStarted = Promise.withResolvers<void>();
  f.world.streams.writeMulti
    .mockImplementationOnce(async (_run, _name, values) => {
      await first.promise;
      f.append(values);
    })
    .mockImplementationOnce(async (_run, _name, values) => {
      tailStarted.resolve();
      await last.promise;
      f.append(values);
    });
  try {
    await f.write(1);
    await f.write(2);
    await vi.advanceTimersByTimeAsync(1);
    await f.write(3);
    await f.write(4);
    let durable = false;
    const drained = f.drain().then(() => {
      durable = true;
    });
    expect(durable).toBe(false);
    first.resolve();
    await tailStarted.promise;
    expect(f.values()).toEqual([[1], [2]]);
    expect(durable).toBe(false);
    last.resolve();
    await drained;
    expect(f.values()).toEqual([[1], [2], [3], [4]]);
    expect(f.world.streams.writeMulti).toHaveBeenCalledTimes(2);
    expect(f.world.streams.close).not.toHaveBeenCalled();
  } finally {
    first.resolve();
    last.resolve();
    f.writer.releaseLock();
  }
});

it("surfaces failed batch persistence at the durability barrier and rejects later writes", async () => {
  const f = fixture();
  const failure = new Error("Batch persistence failed");
  f.world.streams.writeMulti.mockRejectedValueOnce(failure);
  const closed = f.writer.closed.catch((error: unknown) => error);
  try {
    await f.write(1);
    await f.write(2);
    await vi.advanceTimersByTimeAsync(1);
    await expect(f.drain()).rejects.toBe(failure);
    await expect(f.write(3)).rejects.toBe(failure);
    expect(await closed).toBe(failure);
    expect(f.values()).toEqual([]);
    expect(f.world.streams.close).not.toHaveBeenCalled();
  } finally {
    f.writer.releaseLock();
  }
});

it("drains accepted bytes on abort without closing the shared stream", async () => {
  const f = fixture();
  const gate = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  f.world.streams.writeMulti.mockImplementationOnce(async (_run, _name, values) => {
    started.resolve();
    await gate.promise;
    f.append(values);
  });
  const reason = new Error("Producer cancelled");
  const closed = f.writer.closed.catch((error: unknown) => error);
  try {
    await f.write(1);
    await f.write(2);
    let aborted = false;
    const aborting = f.writer.abort(reason).then(() => {
      aborted = true;
    });
    await started.promise;
    expect(aborted).toBe(false);
    expect(f.values()).toEqual([]);
    gate.resolve();
    await aborting;
    expect(await closed).toBe(reason);
    expect(f.values()).toEqual([[1], [2]]);
    expect(f.world.streams.close).not.toHaveBeenCalled();
  } finally {
    gate.resolve();
    f.writer.releaseLock();
  }
});
