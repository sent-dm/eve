import { expect, it, vi } from "vitest";
import { attributesFixtureWorkflow } from "#internal/testing/attributes-workflow.js";
import { getWorld, setWorld, start } from "#internal/workflow/runtime.js";

it("finishes a native workflow while its correctly scoped attribute write is held and later fails", async () => {
  const previous = await getWorld();
  const started = Promise.withResolvers<{ runId: string; writer: unknown }>();
  const release = Promise.withResolvers<void>();
  const finished = Promise.withResolvers<void>();
  const world: typeof previous = {
    ...previous,
    events: {
      ...previous.events,
      async create(...args: Parameters<typeof previous.events.create>) {
        const [runId, event] = args;
        if (event.eventType !== "attr_set") return await previous.events.create(...args);
        started.resolve({ runId, writer: event.eventData.writer });
        try {
          await release.promise;
          throw new Error("Attribute transport unavailable");
        } finally {
          finished.resolve();
        }
      },
    },
  };
  const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
  setWorld(world);
  try {
    const run = await start(attributesFixtureWorkflow, { world });
    const sent = await started.promise;
    const result = await run.returnValue;
    expect(result.runId).toBe(run.runId);
    expect(sent).toMatchObject({
      runId: run.runId,
      writer: { type: "step", stepId: result.stepId },
    });
    release.resolve();
    await finished.promise;
    expect(await run.status).toBe("completed");
  } finally {
    release.resolve();
    warning.mockRestore();
    setWorld(previous);
  }
});
