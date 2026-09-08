import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readStartedOwner } from "#execution/inbox/readiness.js";
import type { InboxAddress } from "#execution/inbox/types.js";

const runtime = vi.hoisted(() => ({ getRun: vi.fn() }));
vi.mock("#internal/workflow/runtime.js", () => runtime);
vi.mock("#compiled/@workflow/core/index.js", () => ({ getWritable: vi.fn() }));

beforeEach(() => vi.resetAllMocks());
afterEach(() => vi.useRealTimers());

function nativeRun(stream: ReadableStream<InboxAddress>, status = "running") {
  const readStatus = vi.fn(async () => status);
  const readReturnValue = vi.fn((): Promise<unknown> => {
    throw new Error("Readiness must not await a nonterminal workflow.");
  });
  const run = {
    getReadable: vi.fn(() => stream),
    get status() {
      return readStatus();
    },
    get returnValue() {
      return readReturnValue();
    },
  };
  runtime.getRun.mockReturnValue(run);
  return { run, readStatus, readReturnValue };
}

describe("started owner readiness", () => {
  it("returns the published winner without inspecting native completion", async () => {
    const address = { token: "tool-call", ownerRunId: "winning-run" };
    const stream = new ReadableStream<InboxAddress>({
      start(controller) {
        controller.enqueue(address);
        controller.close();
      },
    });
    const { run, readStatus, readReturnValue } = nativeRun(stream);

    expect(await readStartedOwner("duplicate-run")).toBe(address);
    expect(runtime.getRun).toHaveBeenCalledExactlyOnceWith("duplicate-run");
    expect(run.getReadable).toHaveBeenCalledExactlyOnceWith({
      namespace: "eve.owner",
      startIndex: 0,
    });
    expect(readStatus).not.toHaveBeenCalled();
    expect(readReturnValue).not.toHaveBeenCalled();
    expect(stream.locked).toBe(false);
  });

  it.each(["pending", "running"])(
    "preserves a read failure without awaiting a %s workflow",
    async (status) => {
      const cause = new Error("Storage connection reset");
      const failure = new Error("Owner record unavailable", { cause });
      const stream = new ReadableStream<InboxAddress>({
        start(controller) {
          controller.error(failure);
        },
      });
      const { readStatus, readReturnValue } = nativeRun(stream, status);

      await expect(readStartedOwner("starting-run")).rejects.toBe(failure);
      expect(failure.cause).toBe(cause);
      expect(readStatus).toHaveBeenCalledOnce();
      expect(readReturnValue).not.toHaveBeenCalled();
    },
  );

  it.each(["failed", "cancelled"])(
    "preserves the native %s error and its cause after a read failure",
    async (status) => {
      const stream = new ReadableStream<InboxAddress>({
        start(controller) {
          controller.close();
        },
      });
      const { readStatus, readReturnValue } = nativeRun(stream, status);
      const cause = new Error("Executor initialization failed");
      const failure = new Error(`Workflow ${status}`, { cause });
      readReturnValue.mockRejectedValueOnce(failure);

      await expect(readStartedOwner("failed-run")).rejects.toBe(failure);
      expect(failure.cause).toBe(cause);
      expect(readStatus).toHaveBeenCalledOnce();
      expect(readReturnValue).toHaveBeenCalledOnce();
    },
  );

  it("reports completed publication failure with the storage error as its cause", async () => {
    const stream = new ReadableStream<InboxAddress>({
      start(controller) {
        controller.close();
      },
    });
    const { readReturnValue } = nativeRun(stream, "completed");

    await expect(readStartedOwner("empty-run")).rejects.toMatchObject({
      message: 'Workflow "empty-run" ended without publishing its owner.',
      cause: { message: "Session storage record does not exist." },
    });
    expect(readReturnValue).not.toHaveBeenCalled();
  });

  it("bounds an unpublished owner with the shared read deadline and releases its reader", async () => {
    vi.useFakeTimers();
    const cancel = vi.fn();
    const stream = new ReadableStream<InboxAddress>({ cancel });
    const { readStatus, readReturnValue } = nativeRun(stream);
    const result = expect(readStartedOwner("starting-run")).rejects.toThrow(
      "Session storage read timed out.",
    );

    await vi.advanceTimersByTimeAsync(9_999);
    expect(readStatus).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await result;
    expect(readStatus).toHaveBeenCalledOnce();
    expect(readReturnValue).not.toHaveBeenCalled();
    expect(cancel).toHaveBeenCalledOnce();
    expect(stream.locked).toBe(false);
  });
});
