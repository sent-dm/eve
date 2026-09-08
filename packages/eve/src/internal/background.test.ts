import { beforeEach, expect, it, vi } from "vitest";
import { runInBackground } from "#internal/background.js";
import { waitUntil } from "#compiled/@vercel/functions/index.js";

vi.mock("#compiled/@vercel/functions/index.js", () => ({ waitUntil: vi.fn() }));

beforeEach(() => vi.mocked(waitUntil).mockReset());

it("registers immediately and reports failure through a non-rejecting host promise", async () => {
  const task = Promise.withResolvers<void>();
  const onError = vi.fn();
  expect(runInBackground(task.promise, onError)).toBeUndefined();
  expect(waitUntil).toHaveBeenCalledOnce();
  const error = new Error("Background write failed");
  task.reject(error);
  await expect(vi.mocked(waitUntil).mock.calls[0]![0]).resolves.toBeUndefined();
  expect(onError).toHaveBeenCalledExactlyOnceWith(error);
});

it("contains host registration and reporter failures", async () => {
  const task = Promise.withResolvers<void>();
  vi.mocked(waitUntil).mockImplementationOnce(() => {
    throw new Error("No host scope");
  });
  const onError = vi.fn(() => {
    throw new Error("Reporter failed");
  });
  expect(() => runInBackground(task.promise, onError)).not.toThrow();
  task.reject(new Error("Write failed later"));
  await expect(vi.mocked(waitUntil).mock.calls[0]![0]).resolves.toBeUndefined();
  expect(onError).toHaveBeenCalledTimes(2);
});
