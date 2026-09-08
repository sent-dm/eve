import { defineEval } from "eve/evals";
import { requirePendingTool, respondWhileRunning, waitForInput } from "./live-input.shared.ts";

export default defineEval({
  description:
    "A workflow tool asks the human mid-body; the answer resumes the run and the result settles the call.",
  async test(t) {
    const live = await t.start("WORKFLOW-CONFIRM-START");
    const request = await waitForInput(t, live, {
      display: "confirmation",
      optionIds: ["approve", "cancel"],
      toolName: "confirm_deploy",
    });
    await requirePendingTool(t, live, "confirm_deploy");

    const approved = await respondWhileRunning(t, live, request, "approve");
    approved.expectOk();
    approved.event("action.result", {
      count: 1,
      data: {
        result: {
          kind: "tool-result",
          output: /"approved":true/u,
          toolName: "confirm_deploy",
        },
        status: "completed",
      },
    });
    approved.messageIncludes("WORKFLOW-CONFIRM-RESULT");
    t.noFailedActions();
  },
});
