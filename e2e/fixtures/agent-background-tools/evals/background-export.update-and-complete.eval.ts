import { defineEval, type EveEvalTurn } from "eve/evals";
import { satisfies } from "eve/evals/expect";

const PROGRESS = "EXPORT-PROGRESS";
const RESULT = "EXPORT-COMPLETE";

export default defineEval({
  description:
    "An authored background defineTool posts one task update then completes; the parent sees both.",
  async test(t) {
    const started = await t.send("BACKGROUND-EXPORT-START");
    started.expectOk();
    started.calledTool("export");

    const receipt = started.requireToolCall("export");
    const taskId = readTaskId(receipt.output);
    if (taskId === undefined) throw new Error("export receipt is missing taskId.");

    const sessionId = t.sessionId;
    if (sessionId === undefined) throw new Error("Eval has no parent session id.");

    const events = [...started.events];
    let streamIndex = requireStreamIndex(t, "task updates");
    for (let turn = 0; turn < 2 && !hasCompletion(events); turn++) {
      const live = t.target.watchTurn(sessionId, { startIndex: streamIndex });
      const result = await live.result();
      result.expectOk();
      events.push(...result.events);
      streamIndex = requireStreamIndex(live.session, "task updates");
    }
    await t.require(
      events,
      satisfies(
        (events: typeof started.events) =>
          events.some(
            (event) =>
              event.type === "message.received" &&
              messageText(event.data.message).includes(
                `Background task ${taskId} (export) update: ${PROGRESS}`,
              ),
          ),
        "parent receives the executor update with task identity",
      ),
    );
    await t.require(
      events,
      satisfies(hasCompletion, "parent acknowledges the executor completion"),
    );
    await t.require(
      events,
      satisfies(
        (events: typeof started.events) =>
          events.some(
            (event) =>
              event.type === "message.received" &&
              messageText(event.data.message).includes(
                `Background task ${taskId} (export) is completed.`,
              ) &&
              messageText(event.data.message).includes(RESULT),
          ),
        "parent receives the executor completion with task identity",
      ),
    );
    const received = events
      .filter((event) => event.type === "message.received")
      .map((event) => messageText(event.data.message))
      .join("\n");
    await t.require(
      received,
      satisfies(
        (text: string) =>
          text.indexOf(PROGRESS) >= 0 && text.indexOf(PROGRESS) < text.indexOf(RESULT),
        "the progress delivery precedes completion, including when coalesced",
      ),
    );
    t.noFailedActions();
  },
});

function readTaskId(output: unknown): string | undefined {
  if (output === null || typeof output !== "object") return undefined;
  const taskId = Reflect.get(output, "taskId");
  return typeof taskId === "string" ? taskId : undefined;
}

function requireStreamIndex(
  session: { readonly state?: { readonly streamIndex?: number } },
  operation: string,
): number {
  const streamIndex = session.state?.streamIndex;
  if (streamIndex === undefined) throw new Error(`${operation} has no session stream index.`);
  return streamIndex;
}

function messageText(message: unknown): string {
  if (typeof message === "string") return message;
  if (!Array.isArray(message)) return "";
  return message
    .flatMap((part) =>
      part !== null &&
      typeof part === "object" &&
      Reflect.get(part, "type") === "text" &&
      typeof Reflect.get(part, "text") === "string"
        ? [Reflect.get(part, "text") as string]
        : [],
    )
    .join("\n");
}

function hasCompletion(events: EveEvalTurn["events"]): boolean {
  return events.some(
    (event) =>
      event.type === "message.completed" &&
      (event.data.message ?? "").includes("BACKGROUND-EXPORT-DONE"),
  );
}
