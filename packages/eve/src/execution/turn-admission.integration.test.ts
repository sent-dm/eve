import { describe, expect, it } from "vitest";

import { sessionDirectory } from "#execution/session/directory.js";
import { activeTurnToken } from "#execution/turn/address.js";
import { createTestRuntime } from "#internal/testing/app-harness.js";
import { captureTurnEvents, filterEventsByType } from "#internal/testing/events.js";
import { startTestSession } from "#internal/testing/session.js";
import { getRun, resumeHook } from "#internal/workflow/runtime.js";
import { defineHook } from "#public/definitions/hook.js";
import { createBundledRuntimeCompiledArtifactsSource } from "#runtime/compiled-artifacts-source.js";

describe("input received during model completion", () => {
  it.each(["duplicate", "steer", "timeout"] as const)(
    "admits %s input before settling the active logical turn",
    async (kind) => {
      let delivered = false;
      const app = await createTestRuntime({
        modules: [
          {
            logicalPath: "hooks/admission.ts",
            loadNamespace: async () => ({
              default: defineHook({
                events: {
                  "step.completed": async (event, ctx) => {
                    if (delivered) return;
                    delivered = true;
                    const session = await sessionDirectory.resolveSession(ctx.session.id);
                    const eventId = kind === "duplicate" ? session.initialEventId : kind;
                    // The sender awaits durable admission while the model step is still active.
                    await resumeHook(activeTurnToken(session.sessionId), {
                      eventId,
                      kind: "session.submit",
                      payload: {
                        candidateRunId: event.data.turnId.replace(/^turn_/, ""),
                        submission: {
                          eventId,
                          command:
                            kind === "timeout"
                              ? { kind: "session-timeout" }
                              : {
                                  kind: "send",
                                  payload: {
                                    message: kind === "duplicate" ? "first" : "also this",
                                  },
                                },
                        },
                      },
                    });
                  },
                },
              }),
            }),
          },
        ],
      });
      await app.run(async () => {
        const session = await startTestSession({
          input: { message: "first" },
          sessionTimeoutMs: false,
          serializedContext: {
            "eve.auth": null,
            "eve.bundle": { source: createBundledRuntimeCompiledArtifactsSource() },
            "eve.channel": { kind: "http", state: {} },
            "eve.mode": "conversation",
          },
        });
        const capture = captureTurnEvents(session);
        try {
          const events = await capture.nextTurn();
          expect(delivered).toBe(true);
          const turns = filterEventsByType(events, "turn.started");
          expect(turns).toHaveLength(1);
          const turnId = turns[0]!.data.turnId;
          expect(turnId).toMatch(/^turn_wrun_/);
          expect(
            filterEventsByType(events, "message.received").map((event) => event.data.message),
          ).toEqual(kind === "steer" ? ["first", "also this"] : ["first"]);
          expect(
            filterEventsByType(events, "step.started").map((event) => ({
              turnId: event.data.turnId,
              stepIndex: event.data.stepIndex,
            })),
          ).toEqual((kind === "steer" ? [0, 1] : [0]).map((stepIndex) => ({ turnId, stepIndex })));
          expect(
            filterEventsByType(events, "turn.completed").map((event) => event.data.turnId),
          ).toEqual([turnId]);
          expect(filterEventsByType(events, "turn.cancelled")).toHaveLength(0);
          expect(events.at(-1)?.type).toBe(
            kind === "timeout" ? "session.completed" : "session.waiting",
          );
          await getRun(turnId.replace(/^turn_/, "")).returnValue;
        } finally {
          capture.dispose();
          await session.cancel();
        }
      });
    },
  );
});
