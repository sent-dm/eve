import { describe, expect, it } from "vitest";

import { sessionDirectory } from "#execution/session/directory.js";
import { sessionEvents } from "#execution/session/events.js";
import { dispatchSessionCommand } from "#execution/session/ingress.js";
import { sessionSnapshots } from "#execution/session/snapshots.js";
import { waitForTurnReceipt } from "#execution/turn/admission.js";
import type { AcceptedSubmission, SessionCheckpoint } from "#execution/turn/types.js";
import { createTestRuntime } from "#internal/testing/app-harness.js";
import {
  bootstrapAdmissionHolderWorkflow,
  storageErrorRoundTripWorkflow,
} from "#internal/testing/bootstrap-admission-workflow.js";
import { captureTurnEvents, filterEventsByType } from "#internal/testing/events.js";
import { getRun, resumeHook, start } from "#internal/workflow/runtime.js";
import { createBundledRuntimeCompiledArtifactsSource } from "#runtime/compiled-artifacts-source.js";

describe("admission before the holder starts its first candidate", () => {
  it("recognizes storage failure after the SDK serializes a failed step", async () => {
    const run = await start(storageErrorRoundTripWorkflow);
    await expect(run.returnValue).resolves.toBe(true);
  });

  it.each(["steer", "queue", "cancel"] as const)(
    "preserves bootstrap semantics when an early %s candidate wins",
    async (kind) => {
      const app = await createTestRuntime();
      await app.run(async () => {
        const first: AcceptedSubmission = {
          eventId: "bootstrap",
          command: { kind: "send", payload: { message: "first" } },
          initial: {
            sessionTimeoutMs: false,
            serializedContext: {
              "eve.auth": null,
              "eve.bundle": { source: createBundledRuntimeCompiledArtifactsSource() },
              "eve.channel": { kind: "http", state: {} },
              "eve.mode": "conversation",
            },
          },
        };
        const holder = await start(bootstrapAdmissionHolderWorkflow, [first]);
        const resources = await sessionDirectory.resolveHolder(holder.runId);
        const encoder = new TextEncoder();
        const session = {
          readable: sessionEvents.read(resources.events).pipeThrough(
            new TransformStream({
              transform(event, controller) {
                controller.enqueue(encoder.encode(`${JSON.stringify(event)}\n`));
              },
            }),
          ),
          async cancel() {
            const reset = await dispatchSessionCommand(resources.sessionId, { kind: "reset" });
            await waitForTurnReceipt(reset.run.runId);
            await holder.cancel();
          },
        };
        const capture = captureTurnEvents(session);
        try {
          const early = await dispatchSessionCommand(
            resources.sessionId,
            kind === "cancel"
              ? { kind: "cancel" }
              : { kind: "send", payload: { message: "second" }, turnPolicy: kind },
            "early",
          );
          await waitForTurnReceipt(early.run.runId);
          if (kind === "cancel") {
            expect(await sessionEvents.tailIndex(resources.events)).toBe(-1);
          } else {
            const turns = [await capture.nextTurn()];
            if (kind === "queue") turns.push(await capture.nextTurn());
            expect(
              turns.map((events) =>
                filterEventsByType(events, "message.received").map((event) => event.data.message),
              ),
            ).toEqual(kind === "queue" ? [["first"], ["second"]] : [["first\n\nsecond"]]);
            expect(filterEventsByType(turns.flat(), "step.started")).toHaveLength(
              kind === "queue" ? 2 : 1,
            );
            expect(filterEventsByType(turns.flat(), "session.started")).toHaveLength(1);
            expect(filterEventsByType(turns.flat(), "turn.cancelled")).toHaveLength(0);
          }

          const settledTail = await sessionEvents.tailIndex(resources.events);
          await resumeHook(`test:bootstrap:${holder.runId}`, {});
          const candidateReader = holder
            .getReadable<string>({ namespace: "test.bootstrap.candidate" })
            .getReader();
          let originalRunId: string;
          try {
            const published = await candidateReader.read();
            if (published.done) throw new Error("Fixture did not start its original candidate.");
            originalRunId = published.value;
          } finally {
            await candidateReader.cancel();
            candidateReader.releaseLock();
          }
          const original = await waitForTurnReceipt(originalRunId);
          expect(original.deliveries.bootstrap).toBe(kind === "cancel" ? "retired" : "applied");
          expect(await sessionEvents.tailIndex(resources.events)).toBe(settledTail);

          if (kind === "cancel") {
            expect(await sessionEvents.tailIndex(resources.events)).toBe(-1);
            const next = await dispatchSessionCommand(resources.sessionId, {
              kind: "send",
              payload: { message: "after cancellation" },
            });
            const events = await capture.nextTurn();
            expect(
              filterEventsByType(events, "message.received").map((event) => event.data.message),
            ).toEqual(["after cancellation"]);
            expect(filterEventsByType(events, "session.started")).toHaveLength(1);
            expect(filterEventsByType(events, "turn.started")).toHaveLength(1);
            expect(filterEventsByType(events, "turn.cancelled")).toHaveLength(0);
            await waitForTurnReceipt(next.run.runId);
          }
          const stored = (await sessionSnapshots.open<SessionCheckpoint>(resources.snapshots))
            .latest;
          if (stored?.checkpoint.phase !== "settled") throw new Error("Session did not settle.");
          expect(stored.checkpoint.deliveries).toMatchObject({
            bootstrap: kind === "cancel" ? "retired" : "applied",
            early: "applied",
          });
          expect(await getRun(originalRunId).status).toBe("completed");
        } finally {
          capture.dispose();
          await session.cancel();
        }
      });
    },
  );
});
