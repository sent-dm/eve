import type { EveEvalContext, EveEvalLiveTurn, EveEvalTurn, InputRequest } from "eve/evals";
import { satisfies } from "eve/evals/expect";

export async function waitForInput(
  t: EveEvalContext,
  live: EveEvalLiveTurn,
  expected: {
    readonly toolName: string;
    readonly display?: "confirmation";
    readonly optionIds?: readonly string[];
  },
): Promise<InputRequest> {
  const event = await live.waitForEvent("input.requested", {
    data: {
      requests: (requests) =>
        requests.some((request) => request.action.toolName === expected.toolName),
    },
  });
  const matches = event.data.requests.filter(
    (request) => request.action.toolName === expected.toolName,
  );
  if (matches.length !== 1) throw new Error(`Expected one input request for ${expected.toolName}.`);
  const request = matches[0]!;
  await t.require(
    request,
    satisfies(
      (request: InputRequest) =>
        (expected.display === undefined || request.display === expected.display) &&
        (expected.optionIds === undefined ||
          JSON.stringify(request.options?.map((option) => option.id).sort()) ===
            JSON.stringify([...expected.optionIds].sort())),
      `input request for ${expected.toolName} has the expected presentation and options`,
    ),
  );
  return request;
}

export async function requirePendingTool(
  t: EveEvalContext,
  live: EveEvalLiveTurn,
  toolName: string,
): Promise<void> {
  await t.require(
    live.events,
    satisfies((events: EveEvalLiveTurn["events"]) => {
      const calls = events
        .flatMap((event) => (event.type === "actions.requested" ? event.data.actions : []))
        .filter((action) => action.kind === "tool-call" && action.toolName === toolName);
      return (
        calls.length === 1 &&
        !events.some(
          (event) =>
            event.type === "action.result" && event.data.result.callId === calls[0]!.callId,
        )
      );
    }, `${toolName} remains pending while waiting for the answer`),
  );
}

export async function respondWhileRunning(
  t: EveEvalContext,
  live: EveEvalLiveTurn,
  request: InputRequest,
  optionId: string,
): Promise<EveEvalTurn> {
  if (!request.options?.some((option) => option.id === optionId))
    throw new Error(`Input request does not offer ${optionId}.`);
  const response = await t.target.fetch(`/eve/v1/session/${encodeURIComponent(live.sessionId)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ inputResponses: [{ requestId: request.requestId, optionId }] }),
  });
  if (!response.ok)
    throw new Error(`Input response failed (${response.status}): ${await response.text()}`);
  return await live.result();
}
