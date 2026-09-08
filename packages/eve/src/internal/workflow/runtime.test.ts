import { beforeEach, describe, expect, it, vi } from "vitest";
import { HookNotFoundError } from "#compiled/@workflow/errors/index.js";
import { getRawHookByToken } from "#internal/workflow/runtime.js";

const mocks = vi.hoisted(() => ({
  lookup: vi.fn(),
  getRun: vi.fn(),
  getKey: vi.fn(),
  hydrateHook: vi.fn(),
}));

vi.mock("#compiled/@workflow/core/runtime.js", () => ({
  getHookByToken: mocks.hydrateHook,
  getWorld: async () => ({
    hooks: { getByToken: mocks.lookup },
    runs: { get: mocks.getRun },
    getEncryptionKeyForRun: mocks.getKey,
  }),
}));

beforeEach(() => vi.clearAllMocks());

describe("raw hook ownership lookup", () => {
  it("returns the owner without reading or hydrating opaque hook metadata", async () => {
    const metadata = vi.fn(() => {
      throw new Error("Ownership does not require metadata access");
    });
    const hook = {
      runId: "holder-run",
      token: "provider:conversation",
      get metadata() {
        return metadata();
      },
    };
    mocks.lookup.mockResolvedValue(hook);

    expect((await getRawHookByToken(hook.token)).runId).toBe("holder-run");
    expect(mocks.lookup).toHaveBeenCalledExactlyOnceWith(hook.token);
    expect(metadata).not.toHaveBeenCalled();
    expect(mocks.hydrateHook).not.toHaveBeenCalled();
    expect(mocks.getRun).not.toHaveBeenCalled();
    expect(mocks.getKey).not.toHaveBeenCalled();
  });

  it.each([new HookNotFoundError("missing"), new Error("Storage unavailable")])(
    "preserves storage errors without a second lookup (%s)",
    async (error) => {
      mocks.lookup.mockRejectedValueOnce(error);
      await expect(getRawHookByToken("provider:conversation")).rejects.toBe(error);
      expect(mocks.lookup).toHaveBeenCalledTimes(1);
      expect(mocks.hydrateHook).not.toHaveBeenCalled();
      expect(mocks.getRun).not.toHaveBeenCalled();
      expect(mocks.getKey).not.toHaveBeenCalled();
    },
  );
});
