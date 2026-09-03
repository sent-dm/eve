import { beforeEach, describe, expect, it, vi } from "vitest";

import { createWorkflowRuntime, waitForCommandHookOwner } from "#execution/workflow-runtime.js";
import { startLocalSubagent } from "#subagents/start-local.js";
import { buildSubagentRunInput } from "#subagents/tool.js";

const createSessionMock = vi.fn();

vi.mock("#execution/workflow-runtime.js", () => ({
  createWorkflowRuntime: vi.fn(() => ({ createSession: createSessionMock })),
  waitForCommandHookOwner: vi.fn(),
}));
vi.mock("#subagents/tool.js", () => ({
  buildSubagentRunInput: vi.fn(),
}));

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(buildSubagentRunInput).mockReturnValue({
    childContinuationToken: "child-token",
    runInput: {} as never,
  });
});

describe("startLocalSubagent", () => {
  it("uses the session that wins continuation ownership", async () => {
    createSessionMock.mockResolvedValue({
      events: new ReadableStream(),
      sessionId: "candidate-session",
    });
    vi.mocked(waitForCommandHookOwner).mockResolvedValue({ runId: "winning-session" });

    const outcome = await startLocalSubagent({
      action: {
        callId: "call-1",
        name: "research",
        nodeId: "subagents/research",
        subagentName: "research",
      } as never,
      auth: null,
      bundle: { compiledArtifactsSource: {} } as never,
      capabilities: undefined,
      channelMetadata: undefined,
      currentSession: {} as never,
      fanoutSize: 1,
      initiatorAuth: null,
      parent: {
        continuationToken: "parent-token",
        originAudience: "private",
        lineage: {
          callId: "call-1",
          rootSessionId: "parent-session",
          sessionId: "parent-session",
          turn: { id: "turn-1", sequence: 0 },
        },
      },
      sandboxSessionId: "parent-session",
      session: {} as never,
      source: { description: "Research", type: "local" },
    });

    expect(createWorkflowRuntime).toHaveBeenCalledOnce();
    expect(outcome).toMatchObject({
      address: {
        continuationToken: "child-token",
        sessionId: "winning-session",
      },
      kind: "called",
    });
  });
});
