import { defineEval } from "eve/evals";
import { satisfies } from "eve/evals/expect";
import { waitForInput } from "./live-input.shared.ts";

/**
 * Cancelling the turn cancels the workflow tool run holding it open. The turn
 * settles as cancelled followed by session.waiting, with no failure events,
 * and the session keeps taking messages.
 */
export default defineEval({
  timeoutMs: 60_000,
  description: "Cancelling a turn unwinds its admitted workflow tool before releasing ownership.",
  async test(t) {
    const live = await t.start("WORKFLOW-HOLD-START");
    await waitForInput(t, live, { toolName: "hold_deploy" });

    const cancelled = await live.cancel();
    await t.require(
      cancelled,
      satisfies(
        (value: typeof cancelled) => value.status === "accepted",
        "cancel request is accepted",
      ),
    );

    const turn = await live.result();
    turn.event("turn.cancelled", { count: 1 });
    turn.eventOrder([{ type: "turn.cancelled" }, { type: "session.waiting" }]);
    turn.notEvent("turn.failed");
    turn.notEvent("session.failed");

    const next = await t.send("WORKFLOW-IDLE-PING");
    next.expectOk();
    next.messageIncludes("WORKFLOW-IDLE");
  },
});
