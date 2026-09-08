import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { after, test } from "node:test";
import { pathToFileURL } from "node:url";

const require = createRequire(
  new URL("../../e2e/fixtures/fixture-tasks/package.json", import.meta.url),
);
const sdk = dirname(require.resolve("@workflow/world-postgres"));
const postgresRequire = createRequire(join(sdk, "index.js"));
const { Pool } = postgresRequire("pg");
const { eq, inArray } = postgresRequire("drizzle-orm");
const { migrate } = postgresRequire("drizzle-orm/node-postgres/migrator");
const { SPEC_VERSION_CURRENT } = postgresRequire("@workflow/world");
const { EntityConflictError } = postgresRequire("@workflow/errors");
const { Schema, createClient } = await import(pathToFileURL(join(sdk, "drizzle/index.js")));
const { createEventsStorage } = await import(
  pathToFileURL(process.env.EVE_TEST_POSTGRES_STORAGE ?? join(sdk, "storage.js"))
);
const connectionString = process.env.EVE_TEST_POSTGRES_URL;
if (!connectionString)
  throw new Error("EVE_TEST_POSTGRES_URL must name a disposable test database.");
const pool = new Pool({ connectionString, max: 12 });
const db = createClient(pool);
await migrate(db, { migrationsFolder: join(sdk, "../src/drizzle/migrations") });
const prefix = `hook-claim-${crypto.randomUUID()}`;
const runIds = [];
const queries = [];
let intercept;

function instrument(client) {
  const query = client.query.bind(client);
  client.query = (...args) => {
    const config = args[0];
    const text = typeof config === "string" ? config : config.text;
    const values = typeof config === "string" ? args[1] : (args[1] ?? config.values);
    queries.push(text);
    const proceed = () => query(...args);
    return intercept?.({ text, values, args, proceed }) ?? proceed();
  };
}
for (const client of pool._clients) instrument(client);
pool.on("connect", instrument);
after(async () => {
  intercept = undefined;
  if (runIds.length > 0) {
    await db.delete(Schema.hooks).where(inArray(Schema.hooks.runId, runIds));
    await db.delete(Schema.events).where(inArray(Schema.events.runId, runIds));
    await db.delete(Schema.eventSlots).where(inArray(Schema.eventSlots.runId, runIds));
    await db.delete(Schema.runs).where(inArray(Schema.runs.runId, runIds));
  }
  await pool.end();
});

async function fixture(name, count = 2) {
  const ids = Array.from({ length: count }, (_, index) => `wrun_${prefix}-${name}-${index}`);
  runIds.push(...ids);
  await db.insert(Schema.runs).values(
    ids.map((runId) => ({
      runId,
      deploymentId: "test",
      status: "running",
      workflowName: "test",
      specVersion: SPEC_VERSION_CURRENT,
    })),
  );
  const token = `${prefix}-${name}`;
  const storage = createEventsStorage(db);
  const request = (index, changes = {}) => ({
    eventType: "hook_created",
    correlationId: `hook_${ids[index]}`,
    eventData: { token, ...changes },
    specVersion: SPEC_VERSION_CURRENT,
  });
  return {
    ids,
    token,
    storage,
    request,
    claim: (index, changes) => storage.create(ids[index], request(index, changes)),
    hooks: () => db.select().from(Schema.hooks).where(eq(Schema.hooks.token, token)),
    events: () => db.select().from(Schema.events).where(inArray(Schema.events.runId, ids)),
  };
}

// The unpatched reproduction holds both completed token reads before either
// INSERT. PostgreSQL executes every statement against the real package schema.
function holdBothPreflights() {
  const gate = Promise.withResolvers();
  let reads = 0;
  let completed = 0;
  intercept = ({ text, args, proceed }) => {
    if (!text.startsWith("select") || !text.includes('"workflow_hooks"') || reads++ >= 2) return;
    const finish = async () => {
      if (++completed === 2) gate.resolve();
      await gate.promise;
    };
    const callback = args.at(-1);
    if (typeof callback === "function") {
      args[args.length - 1] = (...values) => {
        void finish().then(() => callback(...values));
      };
      // A sentinel prevents instrument from executing a callback-style query twice.
      proceed();
      return true;
    }
    return proceed().then(async (value) => {
      await finish();
      return value;
    });
  };
}

test("concurrent claims produce exactly one owner with the real nonunique token index", async () => {
  const f = await fixture("race", 8);
  if (process.env.EVE_TEST_POSTGRES_BASELINE === "1") holdBothPreflights();
  try {
    const results = await Promise.all(f.ids.map((_, index) => f.claim(index)));
    assert.equal((await f.hooks()).length, 1);
    assert.equal(results.filter((result) => result.event.eventType === "hook_created").length, 1);
    const winner = results.find((result) => result.hook)?.hook.runId;
    assert.ok(
      results
        .filter((result) => result.event.eventType === "hook_conflict")
        .every((result) => result.event.eventData.conflictingRunId === winner),
    );
  } finally {
    intercept = undefined;
  }
});

test("same-hook replay is idempotent and never records a self-conflict", async () => {
  const f = await fixture("replay", 1);
  const results = await Promise.allSettled([f.claim(0), f.claim(0)]);
  assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
  assert.ok(EntityConflictError.is(results.find((result) => result.status === "rejected").reason));
  assert.deepEqual(
    (await f.events()).map((event) => event.eventType),
    ["hook_created"],
  );
});

test("failed event persistence rolls back the hook claim", async () => {
  const f = await fixture("rollback", 1);
  intercept = ({ text }) => {
    if (text.startsWith('insert into "workflow"."workflow_events"'))
      throw new Error("Injected event failure");
  };
  try {
    await assert.rejects(f.claim(0), /Injected event failure|Failed query/);
  } finally {
    intercept = undefined;
  }
  assert.equal((await f.hooks()).length, 0);
  assert.equal((await f.events()).length, 0);
  assert.equal((await f.claim(0)).event.eventType, "hook_created");
});

test("a claim is not resumable before its creation event commits", async () => {
  const f = await fixture("publication", 1);
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  intercept = ({ text, proceed }) => {
    if (text.startsWith('insert into "workflow"."workflow_events"')) {
      intercept = undefined;
      entered.resolve();
      return release.promise.then(proceed);
    }
  };
  const claim = f.claim(0);
  try {
    await entered.promise;
    assert.equal((await f.hooks()).length, 0);
    await assert.rejects(
      f.storage.create(f.ids[0], {
        eventType: "hook_received",
        correlationId: f.request(0).correlationId,
        eventData: { token: f.token, payload: new Uint8Array() },
        specVersion: SPEC_VERSION_CURRENT,
      }),
      /not found/,
    );
  } finally {
    intercept = undefined;
    release.resolve();
    await claim;
  }
  assert.deepEqual(
    (await f.events()).map((event) => event.eventType),
    ["hook_created"],
  );
});

test("a disposed hook replay cannot resurrect an old claim", async () => {
  const f = await fixture("disposed-replay", 1);
  await f.claim(0);
  await f.storage.create(f.ids[0], {
    eventType: "hook_disposed",
    correlationId: f.request(0).correlationId,
    specVersion: SPEC_VERSION_CURRENT,
  });
  await assert.rejects(f.claim(0), EntityConflictError.is);
  assert.equal((await f.hooks()).length, 0);
  assert.deepEqual((await f.events()).map((event) => event.eventType).sort(), [
    "hook_created",
    "hook_disposed",
  ]);
});

test("same-hook orphan recovery publishes its missing creation event", async () => {
  const f = await fixture("orphan", 1);
  await db.insert(Schema.hooks).values({
    runId: f.ids[0],
    hookId: f.request(0).correlationId,
    token: f.token,
    ownerId: "",
    projectId: "",
    environment: "",
    specVersion: SPEC_VERSION_CURRENT,
  });
  assert.equal((await f.claim(0)).hook.hookId, f.request(0).correlationId);
  assert.deepEqual(
    (await f.events()).map((event) => event.eventType),
    ["hook_created"],
  );
});

test("terminal owners retain their token until retention ends", async () => {
  const f = await fixture("retention", 3);
  await f.claim(0, { tokenRetentionUntil: new Date(Date.now() + 60_000) });
  await db.update(Schema.runs).set({ status: "completed" }).where(eq(Schema.runs.runId, f.ids[0]));
  assert.equal((await f.claim(1)).event.eventType, "hook_conflict");
  await db
    .update(Schema.hooks)
    .set({ tokenRetentionUntil: new Date(0) })
    .where(eq(Schema.hooks.token, f.token));
  assert.equal((await f.claim(2)).event.eventType, "hook_created");
  assert.deepEqual(
    (await f.hooks()).map((hook) => hook.runId),
    [f.ids[2]],
  );
});

test("disposal and a replacement claim agree on one committed owner", async () => {
  const f = await fixture("dispose", 2);
  await f.claim(0);
  const [_, replacement] = await Promise.all([
    f.storage.create(f.ids[0], {
      eventType: "hook_disposed",
      correlationId: f.request(0).correlationId,
      specVersion: SPEC_VERSION_CURRENT,
    }),
    f.claim(1),
  ]);
  assert.equal((await f.events()).filter((event) => event.eventType === "hook_disposed").length, 1);
  const hooks = await f.hooks();
  if (replacement.event.eventType === "hook_created")
    assert.deepEqual(
      hooks.map((hook) => hook.runId),
      [f.ids[1]],
    );
  else {
    assert.equal(replacement.event.eventType, "hook_conflict");
    assert.equal(hooks.length, 0);
  }
});

test("uncontended claim query count includes its transaction barrier", async (t) => {
  const f = await fixture("queries", 1);
  queries.length = 0;
  await f.claim(0);
  t.diagnostic(
    `Uncontended hook claim: ${queries.length} SQL statements (${queries.filter((query) => query.startsWith("select")).length} reads/locks).`,
  );
  assert.equal(queries.filter((query) => query.includes("pg_advisory_xact_lock")).length, 1);
  assert.equal(queries.filter((query) => query.startsWith("begin")).length, 1);
  assert.equal(queries.filter((query) => query === "commit").length, 1);
});
