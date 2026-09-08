import { expect, it } from "vitest";

import { sessionSnapshots } from "#execution/session/snapshots.js";
import { dispatchSessionCommand } from "#execution/session/ingress.js";
import type { SessionCheckpoint } from "#execution/turn/types.js";
import { createTestRuntime } from "#internal/testing/app-harness.js";
import { captureTurnEvents, filterEventsByType } from "#internal/testing/events.js";
import { startTestSession } from "#internal/testing/session.js";
import { getRun } from "#internal/workflow/runtime.js";
import { mockSandbox, type MockSandbox } from "#internal/testing/mocks/mock-sandbox.js";
import { defineSandbox } from "#public/definitions/sandbox.js";
import type { SandboxBackend } from "#public/definitions/sandbox-backend.js";
import { createBundledRuntimeCompiledArtifactsSource } from "#runtime/compiled-artifacts-source.js";

it("commits image input and restores its sandbox reference on the next turn", async () => {
  const bytes = Buffer.from(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6NVsAAAAASUVORK5CYII=",
    "base64",
  );
  const sandboxes = new Map<string, MockSandbox>();
  const hydrated: Uint8Array[] = [];
  const backend: SandboxBackend = {
    name: "image-test",
    async create(input) {
      let sandbox = sandboxes.get(input.sessionKey);
      if (sandbox === undefined) {
        sandbox = mockSandbox({ id: input.sessionKey });
        sandboxes.set(input.sessionKey, sandbox);
      }
      const stored = sandbox.session;
      const session = {
        ...stored,
        async readBinaryFile(options: Parameters<typeof stored.readBinaryFile>[0]) {
          const value = await stored.readBinaryFile(options);
          if (value !== null) hydrated.push(value);
          return value;
        },
      };
      return {
        session,
        useSessionFn: async () => session,
        captureState: async () => ({
          backendName: "image-test",
          metadata: {},
          sessionKey: input.sessionKey,
        }),
        delete: async () => {
          sandboxes.delete(input.sessionKey);
        },
        stop: async () => {},
        shutdown: async () => {},
      };
    },
    prewarm: async () => ({ reused: true }),
  };
  const app = await createTestRuntime({
    modules: [
      {
        logicalPath: "sandbox.ts",
        loadNamespace: async () => ({ default: defineSandbox({ backend }) }),
      },
    ],
  });
  await app.run(async () => {
    const session = await startTestSession({
      input: {
        message: [
          { type: "text", text: "Describe this image." },
          {
            type: "file",
            mediaType: "image/png",
            data: bytes,
          },
        ],
      },
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
      const initial = await capture.nextTurn();
      expect(filterEventsByType(initial, "turn.completed")).toHaveLength(1);
      expect(filterEventsByType(initial, "session.failed")).toHaveLength(0);
      const turnId = filterEventsByType(initial, "turn.started")[0]!.data.turnId;
      await getRun(turnId.replace(/^turn_/, "")).returnValue;
      const initialHydrations = hydrated.length;

      const next = await dispatchSessionCommand(session.sessionId, {
        kind: "send",
        payload: { message: "What did I send?" },
      });
      const followup = await capture.nextTurn();
      expect(filterEventsByType(followup, "turn.completed")).toHaveLength(1);
      expect(filterEventsByType(followup, "session.failed")).toHaveLength(0);
      await next.run.returnValue;
      expect(hydrated.length).toBeGreaterThan(initialHydrations);
      expect(hydrated.at(-1)).toEqual(bytes);

      const latest = (await sessionSnapshots.open<SessionCheckpoint>(session.resources.snapshots))
        .latest;
      const checkpoint = latest?.checkpoint;
      if (checkpoint?.phase !== "settled") throw new Error("Image session did not settle.");
      expect(checkpoint.state.snapshot.session.sandboxState?.session).toBeDefined();
      const first = checkpoint.state.snapshot.session.history[0];
      expect(first?.role).toBe("user");
      const attachment = Array.isArray(first?.content)
        ? first.content.find((part) => part.type === "file")
        : undefined;
      expect(attachment?.mediaType).toBe("image/png");
      expect(String(attachment?.data)).toMatch(/^eve-sandbox:/);
    } finally {
      capture.dispose();
      await session.cancel();
    }
  });
});
