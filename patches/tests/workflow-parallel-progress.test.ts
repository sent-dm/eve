import { EntityConflictError, FatalError } from "@workflow/errors";
import { SPEC_VERSION_CURRENT, slotToEventId, type Event, type WorkflowRun } from "@workflow/world";
import { afterEach, expect, it, vi } from "vitest";
import { workflowEntrypoint } from "#workflow-sdk/runtime.js";
import { setWorld } from "#workflow-sdk/runtime/world.js";
import { registerStepFunction } from "#workflow-sdk/private.js";
import {
  dehydrateWorkflowArguments,
  dehydrateStepReturnValue,
  hydrateStepArguments,
} from "#workflow-sdk/serialization.js";

vi.mock("@vercel/functions", () => ({
  waitUntil: vi.fn((p: Promise<unknown>) => {
    void p.catch(() => {});
  }),
}));
vi.mock("@workflow/utils/get-port", () => ({ getPort: vi.fn().mockResolvedValue(3000) }));
afterEach(() => {
  setWorld(undefined);
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

type Queued = { messageId: string; message: Record<string, unknown>; options: any };
async function scenario(
  body: string,
  functions: Record<string, (...args: any[]) => Promise<unknown>>,
  options: {
    batch?: boolean;
    failPublish?: () => Promise<void>;
    beforeRecord?: (request: any) => Promise<void>;
  } = {},
) {
  const runId = "wrun_parallel_progress";
  const now = new Date();
  const run: WorkflowRun = {
    runId,
    workflowName: "workflow",
    attributes: {},
    status: "running",
    input: await dehydrateWorkflowArguments([], runId, undefined, []),
    createdAt: now,
    updatedAt: now,
    startedAt: now,
    deploymentId: "test-deployment",
    specVersion: SPEC_VERSION_CURRENT,
  };
  const events: Event[] = [];
  const steps = new Map<string, any>();
  const queued: Queued[] = [];
  const published = new Set<string>();
  const invocations = new Set<Promise<unknown>>();
  function record(request: any): Event {
    const event = {
      ...request,
      runId,
      specVersion: SPEC_VERSION_CURRENT,
      eventId: slotToEventId(events.length + 1),
      createdAt: new Date(),
    } as Event;
    events.push(event);
    return event;
  }
  record({
    eventType: "run_created",
    eventData: { deploymentId: run.deploymentId, workflowName: run.workflowName, input: run.input },
  });
  record({ eventType: "run_started" });
  const create = vi.fn(async (_id: string, request: any) => {
    if (request.eventType === "run_started")
      return { run, events: [...events], cursor: String(events.length), hasMore: false };
    const id = request.correlationId;
    if (request.eventType === "step_created") {
      if (steps.has(id)) throw new EntityConflictError("Step exists");
      const event = record(request);
      const step = {
        runId,
        stepId: id,
        stepName: request.eventData.stepName,
        status: "pending",
        attempt: 0,
        input: request.eventData.input,
        createdAt: event.createdAt,
        updatedAt: event.createdAt,
      };
      steps.set(id, step);
      return { event, step };
    }
    if (request.eventType === "step_started") {
      let step = steps.get(id);
      const stepCreated = !step;
      if (!step) {
        const data = request.eventData;
        if (!data?.input || !data?.stepName) throw new Error("Step not found");
        const created = record({
          eventType: "step_created",
          correlationId: id,
          eventData: { stepName: data.stepName, input: data.input },
        });
        step = {
          runId,
          stepId: id,
          stepName: data.stepName,
          input: data.input,
          status: "pending",
          attempt: 0,
          createdAt: created.createdAt,
          updatedAt: created.createdAt,
        };
        steps.set(id, step);
      }
      if (step.status !== "pending")
        throw new EntityConflictError("Step already running or terminal");
      const event = record(request);
      Object.assign(step, {
        status: "running",
        attempt: step.attempt + 1,
        startedAt: event.createdAt,
        updatedAt: event.createdAt,
      });
      return { event, step: { ...step }, stepCreated };
    }
    if (request.eventType === "step_completed" || request.eventType === "step_failed") {
      const step = steps.get(id);
      if (!step || step.status !== "running") throw new EntityConflictError("Step terminal");
      Object.assign(step, {
        status: request.eventType === "step_completed" ? "completed" : "failed",
        completedAt: new Date(),
      });
    }
    if (request.eventType === "run_completed" || request.eventType === "run_failed") {
      if (run.status !== "running") throw new EntityConflictError("Run terminal");
      run.status = request.eventType === "run_completed" ? "completed" : "failed";
    }
    await options.beforeRecord?.(request);
    return { event: record(request) };
  });
  const list = vi.fn(async ({ pagination }: any) => {
    const offset = Number(pagination?.cursor ?? 0);
    return { data: events.slice(offset), cursor: String(events.length), hasMore: false };
  });
  const deliveryInputs = new Map<string, Record<string, unknown>>();
  let delivery: (message: Record<string, unknown>, id: string) => Promise<unknown>;
  const fixtureOptions = options;
  const queue = vi.fn(async (_name: string, message: Record<string, unknown>, options: any) => {
    await fixtureOptions.failPublish?.();
    const key = options?.idempotencyKey;
    if (!key || !published.has(key)) {
      if (key) published.add(key);
      queued.push({ messageId: `msg_${queued.length}`, message, options });
    }
    return { messageId: null };
  });
  setWorld({
    specVersion: SPEC_VERSION_CURRENT,
    getDeploymentId: async () => run.deploymentId,
    createQueueHandler: (_prefix: string, handler: any) => {
      delivery = (message, messageId) =>
        handler(message, {
          requestId: `req_${messageId}`,
          attempt: 1,
          queueName: "__wkf_workflow_workflow",
          messageId,
        });
      return async (request: Request) => {
        const id = request.headers.get("x-test-message")!;
        return await delivery(deliveryInputs.get(id)!, id);
      };
    },
    events: {
      create,
      list,
      ...(options.batch
        ? {
            createBatch: async (id: string, entries: any[]) => ({
              results: await Promise.all(
                entries.map(async (entry) => {
                  try {
                    return await create(id, entry.event);
                  } catch (error) {
                    if (EntityConflictError.is(error))
                      return { error: "conflict", status: 409, message: error.message };
                    throw error;
                  }
                }),
              ),
            }),
          }
        : {}),
    },
    runs: { get: async () => ({ ...run }) },
    queue,
    getEncryptionKeyForRun: async () => undefined,
  } as any);
  for (const [name, fn] of Object.entries(functions)) registerStepFunction(name, fn);
  const declarations = Object.keys(functions)
    .map((name) => `const ${name} = globalThis[Symbol.for('WORKFLOW_USE_STEP')]('${name}');`)
    .join("\n");
  const handler = workflowEntrypoint(
    `${declarations}\nasync function workflow() { ${body} }\nglobalThis.__private_workflows = new Map([['workflow', workflow]]);`,
  );
  const start = () => invoke({ runId, requestedAt: now }, "msg_first");
  const invoke = (message: Record<string, unknown>, messageId: string) => {
    deliveryInputs.set(messageId, message);
    const promise = handler(
      new Request("https://test.example", {
        method: "POST",
        headers: { "content-type": "application/json", "x-test-message": messageId },
        body: JSON.stringify(message),
      }),
    );
    invocations.add(promise);
    void promise.finally(() => invocations.delete(promise)).catch(() => {});
    return promise;
  };
  let nextDelivery = 0;
  const dispatch = () => {
    const batch = queued.slice(nextDelivery);
    nextDelivery = queued.length;
    return batch
      .filter((item) => !item.options?.delaySeconds)
      .map((item) => invoke(item.message, item.messageId));
  };
  const output = async () => {
    const completed = events.find((e) => e.eventType === "run_completed");
    return completed?.eventType === "run_completed"
      ? await hydrateStepArguments(completed.eventData.output, runId, undefined, [])
      : undefined;
  };
  const inject = async (payload: unknown) => {
    const hook = events.find((e) => e.eventType === "hook_created");
    if (!hook) throw new Error("No hook created");
    record({
      eventType: "hook_received",
      correlationId: hook.correlationId,
      eventData: {
        token: "control",
        payload: await dehydrateStepReturnValue(payload, runId, undefined, []),
      },
    });
  };
  return {
    start,
    dispatch,
    invoke,
    inject,
    events,
    queued,
    steps,
    create,
    list,
    queue,
    output,
    invocations,
  };
}

async function settledWithin<T>(
  promise: Promise<T>,
  milliseconds = 200,
): Promise<{ settled: true; value: T } | { settled: false }> {
  let timer: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([
      promise.then((value) => ({ settled: true as const, value })),
      new Promise<{ settled: false }>((resolve) => {
        timer = setTimeout(() => resolve({ settled: false }), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer!);
  }
}

it.each(["single", "batch", "resilient"] as const)(
  "continues foreground steps while an independent long-running sibling is pending (%s)",
  async (mode) => {
    if (mode === "resilient") vi.stubEnv("WORKFLOW_RESILIENT_STEP_DISPATCH", "1");
    const watcherGate = Promise.withResolvers<string>();
    const watcherStarted = Promise.withResolvers<void>();
    const shortDone = Promise.withResolvers<void>();
    const progressed = Promise.withResolvers<void>();
    const watcher = vi.fn(async () => {
      watcherStarted.resolve();
      return await watcherGate.promise;
    });
    const short = vi.fn(async () => {
      shortDone.resolve();
      return "hash";
    });
    const next = vi.fn(async (value: unknown) => {
      progressed.resolve();
      return value;
    });
    const test = await scenario(
      `const pending = watcher(); const value = await short(); await next(value); return await pending;`,
      { watcher, short, next },
      { batch: mode === "batch" },
    );
    const first = test.start();
    try {
      // Initial suspension must publish parallel steps instead of waiting on them.
      expect(await settledWithin(first)).toEqual({ settled: true, value: undefined });
      const attempts = test.dispatch();
      await watcherStarted.promise;
      await shortDone.promise;
      // The short step must replay and publish its dependent next() before watcher settles.
      expect(await settledWithin(attempts[1]!)).toEqual({ settled: true, value: undefined });
      await Promise.all(test.dispatch());
      expect(await settledWithin(progressed.promise)).toEqual({ settled: true, value: undefined });
      expect(watcher).toHaveBeenCalledTimes(1);
      expect(short).toHaveBeenCalledTimes(1);
      expect(next).toHaveBeenCalledExactlyOnceWith("hash");
      expect(await test.output()).toBeUndefined();
      watcherGate.resolve("closed");
      await Promise.all(attempts);
      expect(await test.output()).toBe("closed");
    } finally {
      watcherGate.resolve("closed");
      await Promise.allSettled(test.invocations);
    }
  },
);

it("retains the one-step inline fast path", async () => {
  const short = vi.fn(async () => "done");
  const test = await scenario("return await short();", { short });
  await test.start();
  expect(await test.output()).toBe("done");
  expect(short).toHaveBeenCalledTimes(1);
  expect(test.queue).not.toHaveBeenCalled();
});

it("observes the winning Promise.race result without waiting for the losing step", async () => {
  const gate = Promise.withResolvers<string>();
  const watcher = vi.fn(async () => await gate.promise);
  const short = vi.fn(async () => "short");
  const test = await scenario("return await Promise.race([watcher(), short()]);", {
    watcher,
    short,
  });
  await test.start();
  const attempts = test.dispatch();
  try {
    expect(await settledWithin(attempts[1]!)).toEqual({ settled: true, value: undefined });
    expect(await test.output()).toBe("short");
    expect(watcher).toHaveBeenCalledTimes(1);
  } finally {
    gate.resolve("watcher");
    await Promise.allSettled(attempts);
  }
});

it("delivers a caught foreground failure before a pending sibling finishes", async () => {
  const gate = Promise.withResolvers<string>();
  const watcher = vi.fn(async () => await gate.promise);
  const short = vi.fn(async () => {
    throw new FatalError("Expected failure");
  });
  const next = vi.fn(async (message: unknown) => message);
  const test = await scenario(
    `const pending = watcher(); try { await short(); } catch (error) { await next(error.message); } return await pending;`,
    { watcher, short, next },
  );
  await test.start();
  const attempts = test.dispatch();
  try {
    expect(await settledWithin(attempts[1]!)).toEqual({ settled: true, value: undefined });
    await Promise.all(test.dispatch());
    expect(next).toHaveBeenCalledExactlyOnceWith("Expected failure");
  } finally {
    gate.resolve("closed");
    await Promise.allSettled(attempts);
  }
});

it("does not execute an unclaimed body when parallel dispatch publication fails", async () => {
  const first = vi.fn(async () => "first");
  const second = vi.fn(async () => "second");
  const failure = new Error("Queue unavailable");
  const test = await scenario(
    "return await Promise.all([first(), second()]);",
    { first, second },
    {
      failPublish: async () => {
        throw failure;
      },
    },
  );
  await expect(test.start()).rejects.toThrow("Queue unavailable");
  expect(first).not.toHaveBeenCalled();
  expect(second).not.toHaveBeenCalled();
  expect(test.events.filter((e) => e.eventType === "step_started")).toHaveLength(0);
  expect(test.events.filter((e) => e.eventType === "step_created")).toHaveLength(2);
});

it("schedules a durable wait after foreground completion while a sibling remains pending", async () => {
  const gate = Promise.withResolvers<string>();
  const watcher = vi.fn(async () => await gate.promise);
  const short = vi.fn(async () => "hash");
  const test = await scenario(
    `const pending = watcher(); await short(); await globalThis[Symbol.for('WORKFLOW_SLEEP')]('1 hour'); return await pending;`,
    { watcher, short },
  );
  await test.start();
  const attempts = test.dispatch();
  try {
    expect(await settledWithin(attempts[1]!)).toEqual({ settled: true, value: undefined });
    expect(test.events.filter((e) => e.eventType === "wait_created")).toHaveLength(1);
    expect(test.queued.some((q) => q.message.waitContinuation)).toBe(true);
    expect(watcher).toHaveBeenCalledTimes(1);
  } finally {
    gate.resolve("closed");
    await Promise.allSettled(attempts);
  }
});

it("preserves an interleaved hook payload before the foreground step continuation", async () => {
  const gate = Promise.withResolvers<string>();
  const watcher = vi.fn(async () => await gate.promise);
  const short = vi.fn(async () => "hash");
  const next = vi.fn(async (messages: unknown) => messages);
  let test: Awaited<ReturnType<typeof scenario>>;
  let injected = false;
  test = await scenario(
    `
    const hook = globalThis[Symbol.for('WORKFLOW_CREATE_HOOK')]({ token: 'control' });
    await hook.getConflict();
    const received = [];
    void (async () => { for await (const value of hook) received.push(value); })();
    const pending = watcher();
    await short();
    await next(received);
    return await pending;
  `,
    { watcher, short, next },
    {
      beforeRecord: async (request) => {
        if (!injected && request.eventType === "step_completed") {
          injected = true;
          await test.inject("cancel");
        }
      },
    },
  );
  await test.start();
  const attempts = test.dispatch();
  try {
    expect(await settledWithin(attempts[1]!)).toEqual({ settled: true, value: undefined });
    await Promise.all(test.dispatch());
    expect(next).toHaveBeenCalledExactlyOnceWith(["cancel"]);
    const receivedIndex = test.events.findIndex((e) => e.eventType === "hook_received");
    const completedIndex = test.events.findIndex((e) => e.eventType === "step_completed");
    expect(receivedIndex).toBeLessThan(completedIndex);
  } finally {
    gate.resolve("closed");
    await Promise.allSettled(attempts);
  }
});

it("keeps duplicate watcher deliveries single-flight while another completion advances the workflow", async () => {
  const gate = Promise.withResolvers<string>();
  const started = Promise.withResolvers<void>();
  const watcher = vi.fn(async () => {
    started.resolve();
    return await gate.promise;
  });
  const short = vi.fn(async () => "hash");
  const next = vi.fn(async () => "progress");
  const test = await scenario(
    "const pending = watcher(); await short(); await next(); return await pending;",
    { watcher, short, next },
  );
  await test.start();
  const attempts = test.dispatch();
  await started.promise;
  const message = test.queued.find((q) => q.message.stepName === "watcher")!;
  const duplicate = test.invoke(message.message, "msg_duplicate");
  try {
    expect(await settledWithin(attempts[1]!)).toEqual({ settled: true, value: undefined });
    await Promise.all(test.dispatch());
    expect(next).toHaveBeenCalledTimes(1);
    expect(watcher).toHaveBeenCalledTimes(1);
    expect(await settledWithin(duplicate, 20)).toEqual({ settled: false });
    gate.resolve("closed");
    await Promise.all([...attempts, duplicate]);
    expect(watcher).toHaveBeenCalledTimes(1);
    expect(
      test.events.filter(
        (e) => e.eventType === "step_started" && e.correlationId === message.message.stepId,
      ),
    ).toHaveLength(1);
    expect(await test.output()).toBe("closed");
  } finally {
    gate.resolve("closed");
    await Promise.allSettled([...attempts, duplicate]);
  }
});
