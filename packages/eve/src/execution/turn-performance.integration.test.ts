import { describe, expect, it, vi } from "vitest";

import { dispatchSessionCommand } from "#execution/session/ingress.js";
import { createTestRuntime } from "#internal/testing/app-harness.js";
import { captureTurnEvents } from "#internal/testing/events.js";
import { startTestSession } from "#internal/testing/session.js";
import { getRun, getWorld } from "#internal/workflow/runtime.js";
import { createBundledRuntimeCompiledArtifactsSource } from "#runtime/compiled-artifacts-source.js";

describe("ordinary turn storage budget", () => {
  it("keeps ordinary turns within the holder storage call budget", async () => {
    const app = await createTestRuntime();
    const measured = await app.run(async () => {
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
      const world = await getWorld();
      const samples: Array<{
        calls: Record<string, number>;
        clientMs: number;
        completionMs: number;
        hookCreationPasses: number;
      }> = [];
      const spies = [
        vi.spyOn(world.runs, "get"),
        vi.spyOn(world.streams, "get"),
        vi.spyOn(world.streams, "getInfo"),
        vi.spyOn(world.streams, "write"),
        vi.spyOn(world.streams, "writeMulti"),
        vi.spyOn(world.streams, "close"),
      ];
      const methods = [
        "runs.get",
        "streams.get",
        "streams.getInfo",
        "streams.write",
        "streams.writeMulti",
        "streams.close",
      ];
      const eventCreates = vi.spyOn(world.events, "create");
      try {
        const initialEvents = await capture.nextTurn();
        const firstTurn = initialEvents.find((event) => event.type === "turn.started");
        expect(firstTurn).toBeDefined();
        await getRun(firstTurn!.data.turnId.replace(/^turn_/, "")).returnValue;
        for (let index = 0; index < 3; index++) {
          for (const spy of spies) spy.mockClear();
          eventCreates.mockClear();
          const startedAt = performance.now();
          const candidate = await dispatchSessionCommand(session.sessionId, {
            kind: "send",
            payload: { message: `follow-up ${index}` },
          });
          const events = await capture.nextTurn();
          const clientMs = performance.now() - startedAt;
          await candidate.run.returnValue;
          let replay = 0;
          const hookCreations = eventCreates.mock.calls.flatMap(([runId, event]) => {
            if (runId !== candidate.run.runId) return [];
            if (event.eventType === "run_started") replay++;
            return event.eventType === "hook_created" ? [replay] : [];
          });
          expect(hookCreations).toEqual([1, 1]);
          let activation = 0;
          const stepClaims = eventCreates.mock.calls.flatMap(([runId, event]) => {
            if (runId !== candidate.run.runId) return [];
            if (event.eventType === "run_started") activation++;
            return event.eventType === "step_started" ? [activation] : [];
          });
          expect(stepClaims[0]).toBe(1);
          const calls = Object.fromEntries(
            spies.map((spy, index) => [
              methods[index]!,
              spy.mock.calls.filter((args) => args[0] === session.sessionId).length,
            ]),
          );
          samples.push({
            calls,
            clientMs,
            completionMs: performance.now() - startedAt,
            hookCreationPasses: new Set(hookCreations).size,
          });
          expect(events.filter((event) => event.type === "step.completed")).toHaveLength(1);
          expect(events.at(-1)?.type).toBe("session.waiting");
          expect(Object.values(calls).reduce((sum, count) => sum + count, 0)).toBeLessThanOrEqual(
            index === 0 ? 13 : 12,
          );
        }
        return samples;
      } finally {
        eventCreates.mockRestore();
        for (const spy of spies) spy.mockRestore();
        capture.dispose();
        await session.cancel();
      }
    });
    process.stderr.write(`EVE_TURN_STORAGE_PROFILE=${JSON.stringify(measured)}\n`);
  });
});
