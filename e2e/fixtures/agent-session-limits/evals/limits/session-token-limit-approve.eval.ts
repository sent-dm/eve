import { defineEval } from "eve/evals";
import { equals, satisfies } from "eve/evals/expect";

/**
 * Approving a session token-limit continuation over HTTP while messages queue
 * behind an active model turn.
 */
export default defineEval({
  tags: ["real-model"],
  description:
    "Messages queued during an active turn preserve one limit prompt, and approve resets the budget.",
  timeoutMs: 90_000,
  async test(t) {
    const active = await t.start(
      'Call the `hold-open` tool exactly once with marker "limit-race". Wait for its result, then reply with exactly "approved".',
    );
    await active.waitForEvent("actions.requested");

    const deliverWhileActive = async (message: string): Promise<void> => {
      const response = await t.target.fetch(
        `/eve/v1/session/${encodeURIComponent(active.sessionId)}`,
        {
          body: JSON.stringify({ message, turnPolicy: "queue" }),
          headers: { "content-type": "application/json" },
          method: "POST",
        },
      );
      if (!response.ok) {
        throw new Error(`Concurrent delivery failed (${String(response.status)}).`);
      }
    };

    const messageA = 'Queued message A: preserve the original reply "approved".';
    const messageB = 'Queued message B: preserve the original reply "approved".';
    await deliverWhileActive(messageA);
    await deliverWhileActive(messageB);

    const prompted = await active.result();
    prompted.event("input.requested", { count: 1 });
    const request = t.requireInputRequest({
      display: "confirmation",
      optionIds: ["continue", "stop"],
      toolName: "session_limit_continuation",
    });

    const activeState = t.state;
    if (activeState === undefined) {
      throw new Error("The active eval session did not expose client state.");
    }
    let queuedSession = t.target.watchTurn(active.sessionId, {
      startIndex: activeState.streamIndex,
    });
    const queuedA = await queuedSession.result();
    queuedA.event("message.received", { count: 1, data: { message: messageA } });
    queuedA.event("turn.started", { count: 1 });
    queuedA.event("session.waiting", { count: 1 });
    queuedA.notEvent("input.requested");
    queuedA.notEvent("step.started");
    queuedA.notEvent("message.completed");

    // Each queued submission owns its turn. The unanswered limit prompt
    // defers A, so the second turn carries A followed by B until approval.
    const queuedState = queuedSession.session.state;
    if (queuedState === undefined) {
      throw new Error("The queued eval session did not expose client state.");
    }
    queuedSession = t.target.watchTurn(active.sessionId, {
      startIndex: queuedState.streamIndex,
    });
    const queuedB = await queuedSession.result();
    queuedB.event("message.received", {
      count: 1,
      data: { message: `${messageA}\n\n${messageB}` },
    });
    queuedB.event("turn.started", { count: 1 });
    queuedB.event("session.waiting", { count: 1 });
    queuedB.notEvent("input.requested");
    queuedB.notEvent("step.started");
    queuedB.notEvent("message.completed");
    t.check(
      [...prompted.events, ...queuedA.events, ...queuedB.events].filter(
        (event) => event.type === "input.requested",
      ).length,
      equals(1),
    );

    const resumed = await queuedSession.session.respond([
      {
        optionId: "continue",
        requestId: request.requestId,
      },
    ]);
    resumed.expectOk();
    resumed.event("step.started");
    resumed.notEvent("input.requested");
    resumed.messageIncludes("approved");
    t.check(resumed.sessionId, equals(active.sessionId));
    t.check(queuedSession.session.sessionId, equals(active.sessionId));

    const nextPrompt = await queuedSession.session.send(
      'Post-approval probe: reply with exactly "same session".',
    );
    nextPrompt.event("input.requested", { count: 1 });
    nextPrompt.notEvent("message.completed");
    t.check(nextPrompt.sessionId, equals(active.sessionId));
    t.check(nextPrompt.status, equals("waiting"));
    t.check(queuedSession.session.sessionId, equals(active.sessionId));

    const nextRequest = queuedSession.session.requireInputRequest({
      display: "confirmation",
      optionIds: ["continue", "stop"],
      toolName: "session_limit_continuation",
    });
    t.check(
      nextRequest.requestId,
      satisfies(
        (requestId: string) => requestId !== request.requestId,
        "the next token window creates a distinct continuation request",
      ),
    );
  },
});
