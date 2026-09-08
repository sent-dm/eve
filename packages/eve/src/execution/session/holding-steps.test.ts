import { beforeEach, describe, expect, it, vi } from "vitest";
import { initializeHolderStep, redirectHolderStep } from "#execution/session/holding-steps.js";
import { createSessionResources } from "#execution/session/resources.js";
import type { AcceptedSubmission } from "#execution/turn/types.js";

const mocks = vi.hoisted(() => ({
  initialize: vi.fn(),
  owner: vi.fn(async (runId: string) => ({ runId, deploymentId: "owner-deployment" })),
  publish: vi.fn(),
  resolve: vi.fn(),
  dispatch: vi.fn(),
}));
vi.mock("#execution/session/stream-storage.js", () => ({ resolveStreamOwner: mocks.owner }));
vi.mock("#execution/session/directory.js", () => ({
  initializeSessionResources: mocks.initialize,
  publishSessionDescriptor: mocks.publish,
  sessionDirectory: { resolveHolder: mocks.resolve },
}));
vi.mock("#execution/session/dispatch.js", () => ({ dispatchTurn: mocks.dispatch }));
beforeEach(() => vi.clearAllMocks());

describe("holder bootstrap", () => {
  it("persists the bootstrap seed before publishing resources", async () => {
    const firstTurn: AcceptedSubmission = {
      eventId: "first",
      command: { kind: "send", payload: { message: "Hello" } },
    };
    const resources = await initializeHolderStep("holder", firstTurn);
    expect(mocks.owner).toHaveBeenCalledExactlyOnceWith("holder");
    expect(mocks.owner.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.initialize.mock.invocationCallOrder[0]!,
    );
    expect(mocks.initialize).toHaveBeenCalledWith(resources, firstTurn);
    expect(mocks.publish).toHaveBeenCalledWith(
      { runId: "holder", deploymentId: "owner-deployment" },
      resources,
    );
    expect(mocks.initialize.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.publish.mock.invocationCallOrder[0]!,
    );
  });
  it("publishes canonical resources only after dispatching the losing creation's accepted input", async () => {
    const resources = createSessionResources("winner", "initial", {
      runId: "winner",
      deploymentId: "owner-deployment",
    });
    const submission: AcceptedSubmission = {
      eventId: "loser-message",
      acceptedDeploymentId: "deployment",
      command: { kind: "send", payload: { message: "Hello" } },
    };
    mocks.resolve.mockResolvedValue(resources);
    await redirectHolderStep("loser", "winner", submission);
    expect(mocks.resolve).toHaveBeenCalledWith("winner");
    expect(mocks.dispatch).toHaveBeenCalledWith(
      { sessionId: resources.sessionId, resources },
      submission,
    );
    expect(mocks.owner).toHaveBeenCalledExactlyOnceWith("loser");
    expect(mocks.publish).toHaveBeenCalledWith(
      { runId: "loser", deploymentId: "owner-deployment" },
      resources,
    );
    expect(mocks.dispatch.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.publish.mock.invocationCallOrder[0]!,
    );
    expect(mocks.initialize).not.toHaveBeenCalled();
  });
});
