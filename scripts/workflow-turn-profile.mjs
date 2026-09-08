import { setTimeout as delay } from "node:timers/promises";

const RUN_ID = /^wrun_[A-Z0-9]+$/;
const TIMESTAMPS = ["createdAt", "startedAt", "completedAt"];

export function collectAuthoredTurnTimings(events) {
  const turns = new Map();
  for (const event of events) {
    const runId = event.data?.turnId?.replace(/^turn_/, "");
    if (!RUN_ID.test(runId ?? "")) continue;
    if (event.type === "turn.started") {
      turns.set(runId, { runId, turnNumber: event.data.sequence + 1 });
    }
    const turn = turns.get(runId);
    const at = Date.parse(event.meta?.at);
    if (turn === undefined || !Number.isFinite(at)) continue;
    if (event.type === "step.started") turn.modelStartedAt ??= at;
    if (event.type === "step.completed") turn.modelCompletedAt = at;
  }
  if (turns.size === 0 || turns.size > 200) {
    throw new Error("Expected between 1 and 200 native turn owners in the stress events");
  }
  return [...turns.values()].sort((left, right) => left.turnNumber - right.turnNumber);
}

export async function collectVercelTurnProfile(turns, options) {
  for (const name of ["token", "teamId", "projectId"]) {
    if (!options[name]) throw new Error(`Missing Vercel profile credential: ${name}`);
  }
  if (turns.length === 0 || turns.length > 200 || turns.some((turn) => !RUN_ID.test(turn.runId))) {
    throw new Error("Invalid native turn profile input");
  }
  const records = Array.from({ length: turns.length });
  const requests = [];
  let cursor = 0;
  await Promise.all(
    Array.from({ length: Math.min(4, turns.length) }, async () => {
      while (cursor < turns.length) {
        const index = cursor++;
        const turn = turns[index];
        try {
          const run = await request(`/runs/${turn.runId}?remoteRefBehavior=lazy`);
          const steps = [];
          let nextCursor;
          for (let page = 0; page < 5; page++) {
            const query = new URLSearchParams({ limit: "100", remoteRefBehavior: "lazy" });
            if (nextCursor !== undefined) query.set("cursor", nextCursor);
            const result = await request(`/runs/${turn.runId}/steps?${query}`);
            if (!Array.isArray(result.data)) throw new Error("invalid_steps_response");
            steps.push(...result.data.map(sanitizeStep));
            if (!result.hasMore) break;
            nextCursor = result.cursor;
            if (typeof nextCursor !== "string" || page === 4) throw new Error("steps_page_limit");
          }
          records[index] = { ...turn, run: sanitizeRun(run), steps };
        } catch (error) {
          const code =
            /^(http_\d+|transport_error|invalid_json_response|invalid_steps_response|steps_page_limit)$/.test(
              error.message,
            )
              ? error.message
              : "invalid_native_response";
          records[index] = { ...turn, error: code };
        }
      }
    }),
  );
  return createNativeTurnProfile(records, {
    attempts: requests.length,
    requestDurationMs: summarizeDurations(requests),
  });

  async function request(path) {
    for (let attempt = 0; attempt < 3; attempt++) {
      const start = performance.now();
      let retry = false;
      try {
        const response = await (options.fetch ?? fetch)(
          `https://api.vercel.com/v1/workflow/v2${path}`,
          {
            headers: {
              Authorization: `Bearer ${options.token}`,
              "x-vercel-project-id": options.projectId,
              "x-vercel-team-id": options.teamId,
              "x-vercel-environment": "preview",
            },
            signal: AbortSignal.timeout(30_000),
          },
        );
        if (response.ok) {
          try {
            return await response.json();
          } catch {
            throw new Error("invalid_json_response");
          }
        }
        await response.body?.cancel();
        retry = response.status === 429 || response.status >= 500;
        if (!retry || attempt === 2) throw new Error(`http_${response.status}`);
      } catch (error) {
        if (error.message === "invalid_json_response" || /^http_\d+$/.test(error.message))
          throw error;
        if (attempt === 2) throw new Error("transport_error");
        retry = true;
      } finally {
        requests.push(performance.now() - start);
      }
      if (retry) await (options.delay ?? delay)(250 * (attempt + 1));
    }
  }
}

export function createNativeTurnProfile(records, collection) {
  const turns = records.map((record, index) => {
    const { run, steps = [], ...turn } = record;
    if (run === undefined) return turn;
    const orderedSteps = [...steps].sort(
      (left, right) => timestamp(left.startedAt) - timestamp(right.startedAt),
    );
    const execution = orderedSteps.find((step) => step.name === "executeTurnStep");
    const lastExecution = orderedSteps.findLast((step) => step.name === "executeTurnStep");
    const finalizer = orderedSteps.find((step) => step.name === "finalizeTurnStep");
    const previous = records[index - 1]?.run;
    const metrics = {
      nativeRunMs: duration(run.createdAt, run.completedAt),
      createdToStartedMs: duration(run.createdAt, run.startedAt),
      createdToExecuteMs: duration(run.createdAt, execution?.startedAt),
      executeMs: sumStepDurations(steps, "executeTurnStep"),
      finalizeMs: sumStepDurations(steps, "finalizeTurnStep"),
      executeToFinalizeMs: duration(lastExecution?.completedAt, finalizer?.startedAt),
      forwardMs: sumStepDurations(steps, "forwardSubmissionStep"),
      finalizeToRunCompleteMs: duration(finalizer?.completedAt, run.completedAt),
      authoredModelMs: duration(turn.modelStartedAt, turn.modelCompletedAt),
      executeToAuthoredModelMs: duration(execution?.startedAt, turn.modelStartedAt),
      authoredModelToExecuteCompleteMs: duration(turn.modelCompletedAt, lastExecution?.completedAt),
      previousCompletionOverlapMs: duration(run.createdAt, previous?.completedAt),
    };
    return {
      ...turn,
      complete:
        run.status === "completed" &&
        metrics.nativeRunMs !== undefined &&
        steps.every(
          (step) =>
            step.status === "completed" && duration(step.startedAt, step.completedAt) !== undefined,
        ),
      run,
      steps: orderedSteps,
      forwarded: steps.some((step) => step.name === "forwardSubmissionStep"),
      metrics: Object.fromEntries(
        Object.entries(metrics).filter(([, value]) => value !== undefined),
      ),
    };
  });
  return {
    collection,
    expectedTurns: turns.length,
    completeTurns: turns.filter((turn) => turn.complete).length,
    failedCollections: turns.filter((turn) => turn.error !== undefined).length,
    warmTurns: summarizeTurns(turns.slice(1)),
    turns,
  };
}

export function renderNativeTurnProfile(profile) {
  const lines = [
    "### Native Workflow turn profile",
    "",
    `Coverage: **${profile.completeTurns}/${profile.expectedTurns} completed owners**, ${profile.failedCollections} collection failures. Missing timestamps remain missing; collection does not poll for completion.`,
    "",
    "Native times come from the Workflow API. The model bracket uses authored event timestamps, which do not measure stream persistence or client receipt. Client-visible latency remains the stress measurement above. Collection request latency is diagnostic overhead only.",
    "",
    "| Warm turn phase | Samples | p50 | p95 |",
    "| --- | ---: | ---: | ---: |",
  ];
  const labels = {
    createdToExecuteMs: "Owner created → execute started",
    forwardMs: "Forwarding step bodies (owners with forwarding)",
    executeMs: "Execute step bodies",
    executeToFinalizeMs: "Execute completed → finalizer started",
    finalizeMs: "Finalizer step bodies",
    finalizeToRunCompleteMs: "Finalizer completed → owner completed",
    nativeRunMs: "Owner created → completed",
    authoredModelMs: "Authored model bracket",
    executeToAuthoredModelMs: "Execute started → authored model start",
    authoredModelToExecuteCompleteMs: "Authored model end → execute completed",
  };
  for (const [metric, label] of Object.entries(labels)) {
    const summary = profile.warmTurns.metrics[metric];
    if (summary !== undefined)
      lines.push(
        `| ${label} | ${summary.count} | ${summary.p50Ms.toFixed(1)}ms | ${summary.p95Ms.toFixed(1)}ms |`,
      );
  }
  lines.push(
    "",
    `Warm owners with forwarding: **${profile.warmTurns.forwarded}/${profile.warmTurns.collected}**. Follow-ups created before the previous owner completed: **${profile.warmTurns.overlapping}/${profile.warmTurns.overlapSamples}**. This overlap measures owner lifetime, not the exact hook release time.`,
    "",
  );
  return lines.join("\n");
}

export function summarizeDurations(values) {
  if (values.length === 0) return undefined;
  const sorted = [...values].sort((left, right) => left - right);
  const percentile = (probability) => {
    const position = (sorted.length - 1) * probability;
    const lower = Math.floor(position);
    return sorted[lower] + (sorted[Math.ceil(position)] - sorted[lower]) * (position - lower);
  };
  return {
    count: sorted.length,
    maxMs: sorted.at(-1),
    meanMs: sorted.reduce((total, value) => total + value, 0) / sorted.length,
    minMs: sorted[0],
    p50Ms: percentile(0.5),
    p90Ms: percentile(0.9),
    p95Ms: percentile(0.95),
  };
}

function summarizeTurns(turns) {
  const metrics = {};
  for (const turn of turns) {
    for (const [name, value] of Object.entries(turn.metrics ?? {}))
      (metrics[name] ??= []).push(value);
  }
  return {
    collected: turns.filter((turn) => turn.run !== undefined).length,
    forwarded: turns.filter((turn) => turn.forwarded).length,
    overlapping: turns.filter((turn) => turn.metrics?.previousCompletionOverlapMs > 0).length,
    overlapSamples: metrics.previousCompletionOverlapMs?.length ?? 0,
    metrics: Object.fromEntries(
      Object.entries(metrics).map(([name, values]) => [name, summarizeDurations(values)]),
    ),
  };
}

function sanitizeRun(run) {
  return {
    ...pickTimestamps(run),
    status: run.status,
    workflowCoreVersion: run.executionContext?.workflowCoreVersion,
  };
}

function sanitizeStep(step) {
  return {
    ...pickTimestamps(step),
    name: step.stepName?.split("//").at(-1),
    status: step.status,
    attempt: step.attempt,
  };
}

function pickTimestamps(value) {
  return Object.fromEntries(
    TIMESTAMPS.filter((key) => Number.isFinite(Date.parse(value[key]))).map((key) => [
      key,
      value[key],
    ]),
  );
}

function timestamp(value) {
  return typeof value === "number" ? value : Date.parse(value);
}

function duration(start, end) {
  const value = timestamp(end) - timestamp(start);
  return Number.isFinite(value) ? value : undefined;
}

function sumStepDurations(steps, name) {
  const matches = steps.filter((step) => step.name === name);
  const values = matches.map((step) => duration(step.startedAt, step.completedAt));
  return values.length > 0 && values.every((value) => value !== undefined)
    ? values.reduce((total, value) => total + value, 0)
    : undefined;
}
