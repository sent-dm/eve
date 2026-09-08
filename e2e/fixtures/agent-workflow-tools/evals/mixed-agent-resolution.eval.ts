import { defineEval, type EveEvalTurn } from "eve/evals";
import { satisfies } from "eve/evals/expect";

export default defineEval({
  description:
    "One model step mixes blocking and background workflow tools; blocking results resolve inline and background completion is delivered once.",
  async test(t) {
    const initial = await t.send("WORKFLOW-MIXED-AGENTS-START");
    initial.expectOk();
    initial.calledTool("blocking_agent", { count: 1, status: "completed" });
    initial.calledTool("background_agent", { count: 1, status: "completed" });
    initial.messageIncludes("WORKFLOW-MIXED-AGENTS-INITIAL-RESULT");
    const receipt = initial.requireToolCall("background_agent").output;
    const taskId =
      receipt !== null && typeof receipt === "object" ? Reflect.get(receipt, "taskId") : undefined;
    if (typeof taskId !== "string") throw new Error("Background agent receipt has no task id.");

    const events = [...initial.events];
    if (!hasAcknowledgement(events)) {
      const sessionId = initial.sessionId;
      const streamIndex = t.state?.streamIndex;
      if (sessionId === undefined || streamIndex === undefined)
        throw new Error("Mixed workflow turn has no session stream position.");
      const completed = await t.target.watchTurn(sessionId, { startIndex: streamIndex }).result();
      completed.expectOk();
      events.push(...completed.events);
    }
    await t.require(
      events,
      satisfies((events: typeof initial.events) => {
        const calls = events.flatMap((event) =>
          event.type === "subagent.called" && event.data.name === "workflow-marker"
            ? [event.data]
            : [],
        );
        return (
          calls.length === 2 &&
          ["blocking-agent-call:blocking-child", "background-agent-call:background-child"].every(
            (callId) => calls.filter((call) => call.callId === callId).length === 1,
          )
        );
      }, "each workflow tool invokes its child exactly once"),
    );
    await t.require(
      events,
      satisfies(
        (events: typeof initial.events) =>
          events.filter(
            (event) =>
              event.type === "message.received" &&
              JSON.stringify(event.data.message).includes(
                `Background task ${taskId} (background_agent) is completed.`,
              ) &&
              JSON.stringify(event.data.message).includes("api:background"),
          ).length === 1,
        "the background child completion is delivered once with its task identity",
      ),
    );
    await t.require(
      events,
      satisfies(hasAcknowledgement, "the parent acknowledges background completion"),
    );
    t.succeeded();
    t.noFailedActions();
  },
});

function hasAcknowledgement(events: EveEvalTurn["events"]): boolean {
  return events.some(
    (event) => event.type === "message.completed" && event.data.message === "WORKFLOW-REPORT-ACK",
  );
}
