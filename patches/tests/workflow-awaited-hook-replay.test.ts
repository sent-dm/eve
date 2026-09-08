import { EntityConflictError, PreconditionFailedError } from "@workflow/errors";
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

vi.mock("@vercel/functions", () => ({ waitUntil: vi.fn() }));
vi.mock("@workflow/utils/get-port", () => ({ getPort: vi.fn().mockResolvedValue(3000) }));

afterEach(() => {
  setWorld(undefined);
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

interface Options {
  body?: string;
  conflict?: boolean;
  hookWrite?: () => Promise<void>;
  hookCommitted?: (inject: (payload: unknown) => Promise<void>) => Promise<void>;
  claim?: (inject: (payload: unknown) => Promise<void>) => Promise<void>;
  step?: (value: unknown) => Promise<unknown>;
  readPageSize?: number;
}

async function scenario(options: Options = {}) {
  const runId = "wrun_awaited_hook_replay";
  const now = new Date();
  const run: WorkflowRun = {
    runId,
    attributes: {},
    workflowName: "workflow",
    status: "running",
    input: await dehydrateWorkflowArguments([], runId, undefined, []),
    createdAt: now,
    updatedAt: now,
    startedAt: now,
    deploymentId: "test-deployment",
    specVersion: SPEC_VERSION_CURRENT,
  };
  const events: Event[] = [];
  const order: string[] = [];
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
  const inject = async (value: unknown) => {
    const hook = events.find((event) => event.eventType === "hook_created");
    if (!hook) throw new Error("No committed hook");
    record({
      eventType: "hook_received",
      correlationId: hook.correlationId,
      eventData: {
        token: "owner-token",
        payload: await dehydrateStepReturnValue(value, runId, undefined, []),
      },
    });
    order.push("received");
  };
  const effect = vi.fn(async (value: unknown) => {
    order.push("effect");
    return options.step ? options.step(value) : value;
  });
  registerStepFunction("claimEffect", effect);
  const create = vi.fn(async (_id: string, request: any, _params: any) => {
    order.push(request.eventType);
    if (request.eventType === "run_started")
      return { run, events: [...events], cursor: String(events.length), hasMore: false };
    if (request.eventType === "hook_created") {
      const old = events.find(
        (e) =>
          ["hook_created", "hook_conflict"].includes(e.eventType) &&
          e.correlationId === request.correlationId,
      );
      if (old) throw new EntityConflictError("Hook already created");
      await options.hookWrite?.();
      if (options.conflict)
        return {
          event: record({
            ...request,
            eventType: "hook_conflict",
            eventData: { ...request.eventData, conflictingRunId: "wrun_existing_owner" },
          }),
        };
      const event = record(request);
      await options.hookCommitted?.(inject);
      return { event };
    }
    if (request.eventType === "step_started") {
      await options.claim?.(inject);
      const old = events.find(
        (e) => e.eventType === "step_started" && e.correlationId === request.correlationId,
      );
      if (old) throw new EntityConflictError("Step already claimed");
      const data = request.eventData;
      const created = record({
        eventType: "step_created",
        correlationId: request.correlationId,
        eventData: { stepName: data.stepName, input: data.input },
      });
      return {
        event: record(request),
        stepCreated: true,
        step: {
          runId,
          stepId: request.correlationId,
          stepName: data.stepName,
          status: "running",
          attempt: 1,
          input: data.input,
          createdAt: created.createdAt,
          updatedAt: created.createdAt,
          startedAt: created.createdAt,
        },
      };
    }
    return { event: record(request) };
  });
  const list = vi.fn(async ({ pagination }: any) => {
    order.push("list");
    const start = Number(pagination?.cursor ?? 0);
    const end = Math.min(events.length, start + (options.readPageSize ?? events.length));
    return { data: events.slice(start, end), cursor: String(end), hasMore: end < events.length };
  });
  const queue = vi.fn(async () => ({ messageId: null }));
  setWorld({
    specVersion: SPEC_VERSION_CURRENT,
    getDeploymentId: vi.fn(async () => run.deploymentId),
    createQueueHandler: vi.fn((_prefix, handler) => async (request: Request) => {
      const messageId = request.headers.get("x-test-message")!;
      return await handler(
        { runId, requestedAt: now },
        {
          requestId: `req_${messageId}`,
          attempt: 1,
          queueName: "__wkf_workflow_workflow",
          messageId,
        },
      );
    }),
    events: { create, list },
    runs: { get: vi.fn(async () => run) },
    queue,
    getEncryptionKeyForRun: vi.fn(async () => undefined),
  } as any);
  const handler = workflowEntrypoint(`
    const createHook = globalThis[Symbol.for('WORKFLOW_CREATE_HOOK')];
    const effect = globalThis[Symbol.for('WORKFLOW_USE_STEP')]('claimEffect');
    async function workflow() {
      const hook = createHook({token:'owner-token'});
      let conflict;
      try { conflict = await hook.getConflict(); }
      catch (error) { if (error.name === 'HookConflictError') return 'conflict'; throw error; }
      if (conflict) return 'conflict';
      ${options.body ?? "return await effect('owned');"}
    }
    globalThis.__private_workflows = new Map([['workflow', workflow]]);
  `);
  const deliver = async (messageId = "msg_first") => {
    return await handler(
      new Request("https://test.example", { headers: { "x-test-message": messageId } }),
    );
  };
  const output = async () => {
    const completed = events.find((e) => e.eventType === "run_completed");
    return completed?.eventType === "run_completed"
      ? await hydrateStepArguments(completed.eventData.output, runId, undefined, [])
      : undefined;
  };
  return { deliver, events, order, create, list, queue, effect, output };
}

it("commits an uncontested hook then executes in the same activation through an awaited atomic claim", async () => {
  const gate = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  const test = await scenario({
    claim: async () => {
      started.resolve();
      await gate.promise;
    },
  });
  const invocation = test.deliver();
  await started.promise;
  expect(test.effect).not.toHaveBeenCalled();
  expect(test.events.some((e) => e.eventType === "hook_created")).toBe(true);
  gate.resolve();
  expect(await invocation).toBeUndefined();
  expect(test.effect).toHaveBeenCalledExactlyOnceWith("owned");
  expect(test.create.mock.calls.filter(([, r]) => r.eventType === "run_started")).toHaveLength(1);
  expect(test.queue).not.toHaveBeenCalled();
  expect(await test.output()).toBe("owned");
  expect(test.order.indexOf("hook_created")).toBeLessThan(test.order.indexOf("list"));
  expect(test.order.indexOf("list")).toBeLessThan(test.order.indexOf("step_started"));
});

it("preserves the conflict reinvocation path without running owner effects", async () => {
  const test = await scenario({ conflict: true });
  expect(await test.deliver()).toEqual({ timeoutSeconds: 0 });
  expect(test.effect).not.toHaveBeenCalled();
  expect(test.list).not.toHaveBeenCalled();
  expect(await test.deliver("msg_conflict_replay")).toBeUndefined();
  expect(test.events.some((e) => e.eventType === "run_completed")).toBe(true);
  expect(test.effect).not.toHaveBeenCalled();
});

it("loads a hook payload committed between registration and replay", async () => {
  const test = await scenario({
    body: "return await effect(await hook);",
    hookCommitted: async (inject) => {
      await inject("received-after-claim");
    },
  });
  await test.deliver();
  expect(test.effect).toHaveBeenCalledExactlyOnceWith("received-after-claim");
  expect(await test.output()).toBe("received-after-claim");
  expect(test.queue).not.toHaveBeenCalled();
});

it("replays a stale step claim against a newly received hook without speculative effects", async () => {
  let first = true;
  const test = await scenario({
    body: "return await Promise.race([hook, effect('must-not-run')]);",
    claim: async (inject) => {
      if (!first) return;
      first = false;
      await inject("hook-won");
      throw new PreconditionFailedError("Stale event snapshot");
    },
  });
  await test.deliver();
  expect(test.effect).not.toHaveBeenCalled();
  expect(test.events.some((e) => e.eventType === "run_completed")).toBe(true);
  expect(test.create.mock.calls.filter(([, r]) => r.eventType === "step_started")).toHaveLength(1);
  expect(await test.output()).toBe("hook-won");
  expect(test.queue).not.toHaveBeenCalled();
});

it("does not replay or execute effects after a failed hook write and recovers on redelivery", async () => {
  const failure = new Error("Hook write failed");
  let first = true;
  const test = await scenario({
    hookWrite: async () => {
      if (first) {
        first = false;
        throw failure;
      }
    },
  });
  await expect(test.deliver()).rejects.toBe(failure);
  expect(test.effect).not.toHaveBeenCalled();
  expect(test.list).not.toHaveBeenCalled();
  await test.deliver("msg_redelivery");
  expect(test.effect).toHaveBeenCalledExactlyOnceWith("owned");
});

it("a second activation cannot run a step already claimed by the first activation", async () => {
  const started = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const test = await scenario({
    step: async (value) => {
      started.resolve();
      await release.promise;
      return value;
    },
  });
  const first = test.deliver("msg_owner");
  await started.promise;
  await test.deliver("msg_other");
  expect(test.effect).toHaveBeenCalledTimes(1);
  release.resolve();
  await first;
  expect(test.effect).toHaveBeenCalledTimes(1);
  expect(test.events.filter((e) => e.eventType === "step_started")).toHaveLength(1);
});

it("loads every canonical event page before acting on a received payload", async () => {
  const test = await scenario({
    readPageSize: 1,
    body: "return await effect(await hook);",
    hookCommitted: async (inject) => {
      await inject("after-page-boundary");
    },
  });
  await test.deliver();
  expect(test.effect).toHaveBeenCalledExactlyOnceWith("after-page-boundary");
  expect(
    test.order.slice(0, test.order.indexOf("effect")).filter((item) => item === "list"),
  ).toHaveLength(2);
});

it("recovers a committed hook whose write acknowledgement was lost", async () => {
  const failure = new Error("Hook acknowledgement lost");
  let first = true;
  const test = await scenario({
    hookCommitted: async () => {
      if (first) {
        first = false;
        throw failure;
      }
    },
  });
  await expect(test.deliver()).rejects.toBe(failure);
  expect(test.events.filter((event) => event.eventType === "hook_created")).toHaveLength(1);
  expect(test.effect).not.toHaveBeenCalled();
  await test.deliver("msg_redelivery");
  expect(test.effect).toHaveBeenCalledExactlyOnceWith("owned");
  expect(test.events.filter((event) => event.eventType === "hook_created")).toHaveLength(1);
});

it("reloads a duplicate hook creation verdict in the same activation", async () => {
  const test = await scenario({
    hookCommitted: async () => {
      throw new EntityConflictError("Hook already exists");
    },
  });
  expect(await test.deliver()).toBeUndefined();
  expect(test.effect).toHaveBeenCalledExactlyOnceWith("owned");
  expect(test.queue).not.toHaveBeenCalled();
});

it("charges hook registration time to the existing replay budget before continuing", async () => {
  const now = Date.now();
  const clock = vi.spyOn(Date, "now").mockReturnValue(now);
  vi.stubEnv("WORKFLOW_REPLAY_TIMEOUT_MS", "30000");
  const test = await scenario({
    hookCommitted: async () => {
      clock.mockReturnValue(now + 30000);
    },
  });
  await expect(test.deliver()).rejects.toMatchObject({ name: "ReplayTimeoutRetryError" });
  expect(test.effect).not.toHaveBeenCalled();
  expect(test.list).not.toHaveBeenCalled();
});
