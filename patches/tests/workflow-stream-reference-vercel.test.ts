import type { WorkflowRun } from "@workflow/world";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { createGetEncryptionKeyForRun } from "#world-vercel/encryption.js";
import { instrumentedFetch, resolveVercelApiToken } from "#world-vercel/http-core.js";

vi.mock("#world-vercel/http-core.js", () => ({
  instrumentedFetch: vi.fn(),
  resolveVercelApiToken: vi.fn(),
}));
const LOCAL_MATERIAL = new Uint8Array(32).fill(0x2c);
const OLD_RUN_MATERIAL = new Uint8Array(32).fill(0x3d);

beforeEach(() => {
  vi.stubEnv("VERCEL", "1");
  vi.stubEnv("VERCEL_DEPLOYMENT_ID", "dpl_current");
  vi.stubEnv("VERCEL_DEPLOYMENT_KEY", Buffer.from(LOCAL_MATERIAL).toString("base64"));
  vi.mocked(resolveVercelApiToken).mockResolvedValue("test-authorized-token");
  vi.mocked(instrumentedFetch).mockImplementation(
    async () =>
      new Response(JSON.stringify({ key: Buffer.from(OLD_RUN_MATERIAL).toString("base64") })),
  );
});
afterEach(() => {
  vi.clearAllMocks();
  vi.unstubAllEnvs();
});

it("stock Vercel derives the same key from a Run or an explicit stream-owner deployment", async () => {
  const resolve = createGetEncryptionKeyForRun("project-test")!;
  const material = await resolve("wrun_owner", { deploymentId: "dpl_current" });
  expect(material).toHaveLength(32);
  const run: WorkflowRun = {
    runId: "wrun_owner",
    deploymentId: "dpl_current",
    status: "running",
    workflowName: "owner",
    attributes: {},
    createdAt: new Date(),
    updatedAt: new Date(),
  };
  expect(await resolve(run)).toEqual(material);
  expect(instrumentedFetch).not.toHaveBeenCalled();
  expect(resolveVercelApiToken).not.toHaveBeenCalled();
});

it("stock Vercel requests the actual older owner deployment with authorization", async () => {
  const resolve = createGetEncryptionKeyForRun("project-test", "team-test")!;
  expect(
    new Uint8Array((await resolve("wrun_old_owner", { deploymentId: "dpl_owner_old" }))!),
  ).toEqual(OLD_RUN_MATERIAL);
  expect(resolveVercelApiToken).toHaveBeenCalledTimes(1);
  const request = vi.mocked(instrumentedFetch).mock.calls[0]![0];
  expect(new URL(request.url).pathname).toBe("/v1/workflow/run-key/dpl_owner_old");
  expect(new URL(request.url).searchParams.get("runId")).toBe("wrun_old_owner");
  expect(request.headers.get("Authorization")).toBe("Bearer test-authorized-token");
});

it("a cross-deployment reference cannot fall back to the local key when authorization is unavailable", async () => {
  vi.mocked(resolveVercelApiToken).mockResolvedValue(null);
  const resolve = createGetEncryptionKeyForRun("project-test")!;
  await expect(resolve("wrun_old_owner", { deploymentId: "dpl_owner_old" })).rejects.toThrow(
    "no OIDC token or VERCEL_TOKEN",
  );
  expect(instrumentedFetch).not.toHaveBeenCalled();
});
