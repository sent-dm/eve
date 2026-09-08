import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  collectWorkflowStressMetrics,
  createWorkflowStressReport,
  renderWorkflowStressMarkdown,
} from "./workflow-stress-report.mjs";
import {
  collectAuthoredTurnTimings,
  collectVercelTurnProfile,
  createNativeTurnProfile,
  renderNativeTurnProfile,
} from "./workflow-turn-profile.mjs";

const PREFIX = "EVE_WORKFLOW_STRESS_METRIC=";

test("builds a report from eval artifact metrics", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "eve-workflow-stress-report-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  const artifactDirectory = join(root, "2026-08-31", "evals");
  await mkdir(artifactDirectory, { recursive: true });

  await writeArtifact(artifactDirectory, "sequential.json", {
    fixture: "agent-workflow-stress",
    samples: Array.from({ length: 100 }, (_, index) => ({
      durationMs: 1_000 + index * 10,
      turnNumber: index + 1,
    })),
    scenario: "sequential",
    schemaVersion: 1,
    unit: "milliseconds",
  });
  await writeArtifact(artifactDirectory, "concurrent.json", {
    batches: [
      {
        batchDurationMs: 1_200,
        samples: [
          { durationMs: 1_000, sessionNumber: 1 },
          { durationMs: 1_200, sessionNumber: 2 },
        ],
        turnNumber: 1,
      },
      {
        batchDurationMs: 900,
        samples: [
          { durationMs: 800, sessionNumber: 1 },
          { durationMs: 900, sessionNumber: 2 },
        ],
        turnNumber: 2,
      },
    ],
    fixture: "agent-workflow-stress",
    scenario: "concurrent",
    schemaVersion: 1,
    unit: "milliseconds",
  });

  const metrics = await collectWorkflowStressMetrics(root);
  const report = createWorkflowStressReport(metrics, { sha: "abc123" });
  const markdown = renderWorkflowStressMarkdown(report);

  assert.equal(report.scenarios.sequential.allTurns.count, 100);
  assert.equal(report.scenarios.sequential.allTurns.meanMs, 1_495);
  assert.equal(report.scenarios.sequential.sequentialTurnOrderSlopeMsPerTurn, 10);
  assert.equal(report.scenarios.concurrent.firstTurns.p50Ms, 1_100);
  assert.match(markdown, /Sequential warm turn-order slope: \*\*10\.00 ms\/turn\*\*/);
  assert.match(markdown, /sha=`abc123`/);
});

test("reports completed sequential timing when concurrent eval failed", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "eve-workflow-stress-report-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  await writeArtifact(join(root, "2026-08-31", "evals"), "sequential.json", {
    fixture: "agent-workflow-stress",
    samples: [
      { durationMs: 1_000, turnNumber: 1 },
      { durationMs: 900, turnNumber: 2 },
    ],
    scenario: "sequential",
    schemaVersion: 1,
    unit: "milliseconds",
  });

  const report = createWorkflowStressReport(await collectWorkflowStressMetrics(root));
  assert.deepEqual(report.missingScenarios, ["concurrent"]);
  assert.equal(report.scenarios.concurrent, null);
  assert.equal(report.scenarios.sequential.warmTurns.meanMs, 900);
  assert.match(renderWorkflowStressMarkdown(report), /Partial performance data/);
});

test("does not combine scenarios from different eval runs", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "eve-workflow-stress-report-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  const olderRun = join(root, "2026-08-30", "evals");
  const latestRun = join(root, "2026-08-31", "evals");

  await writeArtifact(olderRun, "sequential.json", sequentialMetric());
  await writeArtifact(olderRun, "concurrent.json", concurrentMetric());
  await writeArtifact(latestRun, "sequential.json", sequentialMetric());

  const metrics = await collectWorkflowStressMetrics(root);
  assert.equal(metrics.concurrent, undefined);
  assert.match(metrics.runDirectory, /2026-08-31/);
});

test("does not reuse older metrics when the latest eval run produced none", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "eve-workflow-stress-report-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  await writeArtifact(join(root, "2026-08-30", "evals"), "sequential.json", sequentialMetric());
  const latest = join(root, "2026-08-31", "evals");
  await mkdir(latest, { recursive: true });
  await writeFile(
    join(latest, "sequential.json"),
    JSON.stringify({ result: { logs: [] }, verdict: "failed" }),
  );
  await assert.rejects(
    collectWorkflowStressMetrics(root),
    /2026-08-31.*missing the sequential scenario/,
  );
});

test("separates authored model timing from native completion and exposes forwarding overlap", () => {
  const records = [
    {
      runId: "wrun_FIRST",
      turnNumber: 1,
      run: nativeRun(0, 500),
      steps: [nativeStep("executeTurnStep", 100, 400)],
    },
    {
      runId: "wrun_SECOND",
      turnNumber: 2,
      modelStartedAt: 700,
      modelCompletedAt: 710,
      run: nativeRun(450, 1_400),
      steps: [
        nativeStep("forwardSubmissionStep", 460, 550),
        nativeStep("executeTurnStep", 600, 1_000),
        nativeStep("finalizeTurnStep", 1_050, 1_200),
      ],
    },
    {
      runId: "wrun_THIRD",
      turnNumber: 3,
      run: { ...nativeRun(1_300, 1_900), completedAt: undefined, status: "running" },
      steps: [{ ...nativeStep("executeTurnStep", 1_350, 1_800), completedAt: undefined }],
    },
  ];
  const report = createNativeTurnProfile(records);
  assert.equal(report.completeTurns, 2);
  assert.equal(report.warmTurns.forwarded, 1);
  assert.equal(report.warmTurns.overlapping, 2);
  assert.equal(report.warmTurns.metrics.executeMs.count, 1);
  assert.equal(report.turns[1].metrics.authoredModelMs, 10);
  assert.equal(report.turns[1].metrics.nativeRunMs, 950);
  assert.equal(report.turns[1].metrics.executeToAuthoredModelMs, 100);
  assert.equal(report.turns[1].metrics.authoredModelToExecuteCompleteMs, 290);
  assert.equal(report.turns[1].metrics.previousCompletionOverlapMs, 50);
  assert.equal(report.turns[2].metrics.nativeRunMs, undefined);
  assert.match(renderNativeTurnProfile(report), /2\/3 completed owners/);
  assert.match(
    renderNativeTurnProfile(report),
    /do not measure stream persistence or client receipt/,
  );
});

test("correlates event timings by turn owner without retaining event content", () => {
  const events = [
    { type: "turn.started", data: { turnId: "turn_wrun_ABC", sequence: 0 }, meta: { at: at(1) } },
    { type: "step.started", data: { turnId: "turn_wrun_ABC" }, meta: { at: at(10) } },
    {
      type: "message.completed",
      data: { turnId: "turn_wrun_ABC", message: "private" },
      meta: { at: at(20) },
    },
    { type: "step.completed", data: { turnId: "turn_wrun_ABC" }, meta: { at: at(25) } },
    { type: "session.waiting", data: {}, meta: { at: at(30) } },
  ];
  assert.deepEqual(collectAuthoredTurnTimings(events), [
    {
      runId: "wrun_ABC",
      turnNumber: 1,
      modelStartedAt: 10,
      modelCompletedAt: 25,
    },
  ]);
});

test("collects with four bounded requests and discards native payloads and credentials", async () => {
  let active = 0;
  let maximumActive = 0;
  let calls = 0;
  const turns = Array.from({ length: 8 }, (_, index) => ({
    runId: `wrun_${index}`,
    turnNumber: index + 1,
  }));
  const report = await collectVercelTurnProfile(turns, {
    token: "secret-token",
    teamId: "team",
    projectId: "project",
    fetch: async (url, options) => {
      calls++;
      maximumActive = Math.max(maximumActive, ++active);
      assert.equal(options.headers.Authorization, "Bearer secret-token");
      assert.match(url, /^https:\/\/api\.vercel\.com\/v1\/workflow\/v2\/runs\/wrun_/);
      await new Promise((resolve) => setImmediate(resolve));
      active--;
      const body = url.includes("/steps?")
        ? {
            data: [
              {
                ...nativeStep("executeTurnStep", 100, 200),
                stepName: "step//eve//executeTurnStep",
                inputRef: "private-input",
              },
            ],
            hasMore: false,
          }
        : {
            ...nativeRun(0, 300),
            outputRef: "private-output",
            encryptionPublicKey: "private-key",
            attributes: { secret: "private-attribute" },
          };
      return Response.json(body);
    },
  });
  assert.equal(calls, 16);
  assert.equal(maximumActive, 4);
  assert.equal(report.completeTurns, 8);
  assert.equal(report.collection.attempts, 16);
  assert.doesNotMatch(JSON.stringify(report), /private-|secret-token|Authorization/);
});

test("retries transient transport failures but reports authorization failures without body or polling", async () => {
  let attempts = 0;
  const report = await collectVercelTurnProfile([{ runId: "wrun_ABC", turnNumber: 1 }], {
    token: "secret-token",
    teamId: "team",
    projectId: "project",
    delay: async () => {},
    fetch: async () => {
      if (++attempts === 1) throw new Error("connection failed with secret-token");
      return new Response("private authorization error", { status: 401 });
    },
  });
  assert.equal(attempts, 2);
  assert.equal(report.failedCollections, 1);
  assert.equal(report.turns[0].error, "http_401");
  assert.doesNotMatch(JSON.stringify(report), /secret-token|private authorization/);
});

test("stops transient API retries after three attempts and leaves completion unknown", async () => {
  let attempts = 0;
  const report = await collectVercelTurnProfile([{ runId: "wrun_ABC", turnNumber: 1 }], {
    token: "token",
    teamId: "team",
    projectId: "project",
    delay: async () => {},
    fetch: async () => {
      attempts++;
      return new Response("unavailable", { status: 503 });
    },
  });
  assert.equal(attempts, 3);
  assert.equal(report.completeTurns, 0);
  assert.equal(report.turns[0].error, "http_503");
  assert.equal(report.turns[0].metrics, undefined);
});

function at(milliseconds) {
  return new Date(milliseconds).toISOString();
}

function nativeRun(start, end) {
  return {
    createdAt: at(start),
    startedAt: at(start + 10),
    completedAt: at(end),
    status: "completed",
  };
}

function nativeStep(name, start, end) {
  return { name, startedAt: at(start), completedAt: at(end), status: "completed", attempt: 1 };
}

async function writeArtifact(directory, name, metric) {
  await mkdir(directory, { recursive: true });
  await writeFile(
    join(directory, name),
    JSON.stringify({ result: { logs: [`${PREFIX}${JSON.stringify(metric)}`] } }),
  );
}

function sequentialMetric() {
  return {
    fixture: "agent-workflow-stress",
    samples: [{ durationMs: 1_000, turnNumber: 1 }],
    scenario: "sequential",
    schemaVersion: 1,
    unit: "milliseconds",
  };
}

function concurrentMetric() {
  return {
    batches: [
      {
        batchDurationMs: 1_000,
        samples: [{ durationMs: 1_000, sessionNumber: 1 }],
        turnNumber: 1,
      },
      {
        batchDurationMs: 900,
        samples: [{ durationMs: 900, sessionNumber: 1 }],
        turnNumber: 2,
      },
    ],
    fixture: "agent-workflow-stress",
    scenario: "concurrent",
    schemaVersion: 1,
    unit: "milliseconds",
  };
}
