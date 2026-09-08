import { defineEval } from "eve/evals";
import { requirePendingTool, respondWhileRunning, waitForInput } from "./live-input.shared.ts";

/**
 * `ask` returns a Promise, so the question composes with `Promise.race` — here
 * against a long deadline. The human answers, the race resolves to their
 * choice, and the run settles the call.
 */
export default defineEval({
  description:
    "A workflow tool races ask against a deadline; the answer wins and settles the call.",
  async test(t) {
    const live = await t.start("WORKFLOW-ESCALATE-START");
    const request = await waitForInput(t, live, {
      display: "confirmation",
      optionIds: ["approve", "cancel"],
      toolName: "escalate_deploy",
    });
    await requirePendingTool(t, live, "escalate_deploy");

    const answered = await respondWhileRunning(t, live, request, "approve");
    answered.expectOk();
    answered.calledTool("escalate_deploy", { output: /"decided":"approved"/u });
    answered.messageIncludes("WORKFLOW-ESCALATE-RESULT");
    t.noFailedActions();
  },
});
