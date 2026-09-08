# Workflow SDK Patch Regression Tests

Run directly against the installed patched SDK, without rebuilding eve's vendored output:

```sh
packages/eve/node_modules/.bin/vitest run --config patches/tests/vitest.unit.config.ts
```

The config resolves dependencies from the eve package. Tests mock only the World transport; they exercise the installed Run implementation and its real stream serialization and encryption. They cover per-instance metadata sharing, concurrent readers/writers, failed-lookup retries, fresh status reads, separate read/write encryption capabilities, namespace and durability-op isolation, and new Run instances across World and serialization boundaries.

These tests accompany the local stream-target cache amendment to the cross-run writable patch. The original upstream writer suite is separately retained upstream. Re-evaluate the amendment and these tests when upgrading the SDK.

The awaited-hook replay suite exercises the real workflow entrypoint, VM, event loader and step executor. It verifies that a successful durable hook registration continues in the same activation, while conflicts, concurrent payloads, stale claims, lost write acknowledgements, competing activations, pagination and replay budgets preserve their behavior. Atomic step ownership remains the barrier before effects.
