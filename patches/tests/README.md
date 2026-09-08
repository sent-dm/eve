# Workflow SDK Patch Regression Tests

Run directly against the installed patched SDK, without rebuilding eve's vendored output:

```sh
packages/eve/node_modules/.bin/vitest run --config patches/tests/vitest.unit.config.ts
```

The config resolves dependencies from the eve package. Tests mock only the World transport; they exercise the installed Run implementation and its real stream serialization and encryption. They cover per-instance metadata sharing, concurrent readers/writers, failed-lookup retries, fresh status reads, separate read/write encryption capabilities, namespace and durability-op isolation, and new Run instances across World and serialization boundaries.

These tests accompany the local stream-target cache amendment to the cross-run writable patch. The original upstream writer suite is separately retained upstream. Re-evaluate the amendment and these tests when upgrading the SDK.

The awaited-hook replay suite exercises the real workflow entrypoint, VM, event loader and step executor. It verifies that a successful durable hook registration continues in the same activation, while conflicts, concurrent payloads, stale claims, lost write acknowledgements, competing activations, pagination and replay budgets preserve their behavior. Atomic step ownership remains the barrier before effects.

The resolved-reference suites exercise plain and encrypted frames, immutable public routing, shared owner handles with independent namespaces, failed authorization and key retries, and fresh World instances. The stock Vercel key resolver is tested with mocked HTTP transport for same- and cross-deployment routing. Explicit references use the existing `getEncryptionKeyForRun(runId, { deploymentId })` overload; ordinary Run access retains its full-Run callback.

## PostgreSQL ownership integration

With a disposable PostgreSQL database available, run:

```sh
EVE_TEST_POSTGRES_URL=postgres://user:password@127.0.0.1:5432/test \
  node --test --test-timeout=30000 patches/tests/workflow-postgres-hook-claim.integration.mjs
```

This suite uses the installed PostgreSQL World's storage implementation and real
migrations. It inserts isolated test runs and removes only its own rows. It
verifies concurrent token claims, replay deduplication, event-write rollback,
creation visibility, disposed-hook replay, orphan recovery, retention expiry,
and concurrent disposal. It also reports SQL statement counts. PostgreSQL CI runs
it once in the `fixture-tasks` job before the fixture evals.

For an upstream before/after reproduction, `EVE_TEST_POSTGRES_STORAGE` can point to
another copy of the package's storage module with its normal dependencies. Set
`EVE_TEST_POSTGRES_BASELINE=1` only with the unpatched implementation: this holds
its first two completed token reads so both claims observe the same empty state,
without changing PostgreSQL's schema or query results.
