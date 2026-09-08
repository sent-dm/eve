# Dependency patches

## Workflow stream writers

`@workflow__core@5.0.0-beta.47.patch` backports `Run#getWritable()` and its shared
forwarded-stream serialization from Workflow commit
[`716fce7`](https://github.com/vercel/workflow/commit/716fce7dc795f50974984d8c533b0b466cf00ba9).
It lets independent turn workflows append to holder-owned streams by run ID.

It also includes two upstream reader fixes needed by snapshot reads: cancellation
must stop reconnecting the reader
([`564ad39`](https://github.com/vercel/workflow/commit/564ad3966c5d16d73fd6fd88acdf01e6f92843aa)),
and constructing a readable for its tail index must avoid opening the stream
([`31dabce`](https://github.com/vercel/workflow/commit/31dabce0c87fa48af210362061324ff0369e094f)).

The patch applies the changes to `src/runtime/run.ts`, `src/runtime.ts`,
`src/runtime/suspension-handler.ts`, and `src/serialization.ts` onto the pinned
package's sources. JavaScript, declarations,
and source maps were regenerated with TypeScript 5.9.3 after verifying that this
reproduced the original package artifacts byte for byte.

pnpm applies the patch during installation. The compiled vendor build fingerprints
the modified SDK artifacts, so existing builds invalidate their cached output.
Published eve packages include the patched SDK in their compiled output; consumers
do not need to configure a dependency patch.

Remove this patch and its `patchedDependencies` entry when upgrading to a Workflow
release containing the API and reader fixes, then regenerate the lockfile and
compiled output. The native storage test covers encrypted contributors appending,
restoring snapshots, and releasing or closing shared streams. The session lifecycle
integration suites exercise unencrypted storage.

### Local amendment for upstream: cache immutable stream ownership

The patch also contains an eve-authored SDK optimization that needs upstream review.
`Run#getReadable()` already resolves owner metadata for decryption, but previously
discarded it; each subsequent `getWritable()` fetched the same owner again. A
snapshot read followed by its attempt and commit writes needed three metadata
requests on one `Run` instance.

The amendment caches only `runId`, `deploymentId`, and `encryptionPublicKey` per
instance. Readers seed that cache from their in-flight metadata lookup, and writers
reuse it. Forwarded write keys are cached separately: public-key writers remain
write-only and never request private read keys. Failed target or write-key lookups
clear the corresponding new cache so an explicit retry can recover.

Status, existence, timestamps, and return-value status reads stay fresh. The read-key
callback still receives its full `WorkflowRun`; a read following a completed
write-only lookup may therefore need another metadata request. Cache contents stay
out of serialized `Run` values. Each instance retains its existing World lifetime;
this does not add support for replacing the global World while reusing an old run.

Run the [patch regression tests](./tests/README.md) against the installed SDK.
They exercise real serialization and encryption with a controlled World transport,
including concurrent access, failed lookups, fresh status, namespaces, durability
promises, and cache isolation. The original upstream writer suite also passed in
the isolated patch build. Retain or re-evaluate this amendment when upgrading;
the upstream writer API alone does not include it.

### Local amendment for upstream: replay a durable hook claim in-process

An awaited `hook.getConflict()` previously persisted its successful registration,
then requeued the workflow solely to observe that registration. The patch instead
joins pending writes and dispatches, clears the retained VM, reloads the canonical
event log, and cold-replays through the existing invocation loop. This removes one
queue handoff and repeated invocation setup from uncontended turn admission.

Hook conflicts still reinvoke. Failed writes reject the delivery. Interleaved hook
payloads retain their canonical order, and the next step still waits for atomic
`step_started` ownership before effects. Existing replay budgets and stale-claim
recovery remain in force; no events are synthesized or accepted optimistically.
The separate QuickJS entrypoint is unchanged.

The installed SDK regression suite exercises the actual entrypoint, VM, event
loader, and step executor against a controlled World. It covers registration and
claim failures, lost acknowledgements, competing activations, concurrent payloads,
pagination, and budget exhaustion. The original upstream runtime/precondition
tests also passed in the isolated build (67 tests including the new cases).
Hosted timing is measured separately; local tests establish scheduling and safety.

### Local amendment for upstream: resolve immutable stream references

`Run#getStreamReference()` now issues a plain reference containing the owner's run
ID, deployment ID, optional namespace and public encryption key.
`Run.fromStreamReference()` reconstructs local read/write accessors without fetching
the owner again. Explicit namespace options override the reference's default.
Private keys and accessor caches remain inside the current execution.

Explicit reference readers use the existing World overload
`getEncryptionKeyForRun(runId, { deploymentId })`. Ordinary Run readers keep their
full-Run callback. The stock Vercel World still authorizes cross-deployment key
requests against the actual owner deployment. A public reference permits sealed
writes, but does not grant private read keys or bypass stream authorization.

The holder resolves routing during its existing initialization step and stores it
inside eve-owned opaque stream IDs. Each step retains one handle per owner, so a
cold descriptor read and subsequent snapshot reads share their existing key cache.
Steps that already have resolved routing avoid owner metadata lookups. Descriptor
discovery still needs its ordinary read and may resolve the holder's input payload.

The installed SDK tests cover immutable references, encrypted and plaintext frames,
namespace sharing, failed-key retries, write-only access, cancellation before the
first read, and stock Vercel key routing. Native storage tests verify owner routing,
encrypted contributor writes, and key-resolution counts across cold and resolved
scopes. This API is a local proposal for upstream review; retain or re-evaluate it
when upgrading the SDK.

### Local amendment for upstream: resume on individual step completion

The Node scheduler previously waited for all inline bodies before replay, and
separately dispatched steps also deferred replay until every sibling completed.
A background step waiting for its parent could therefore block a foreground
continuation that the parent needed, creating a deadlock.

The patch permits new inline execution only for a lone pending step, before any
inline claim is committed. Parallel steps use independent queue messages, and
each terminal result reloads the canonical event log and replays the workflow.
The workflow's promises determine which siblings must finish. Atomic claims,
single-flight protection, ownership leases and retry budgets remain unchanged.

The installed regression suite covers progress, races, failures and event order.
Sequential steps retain their inline fast path; parallel work can use more queue
invocations and replays. Previously created groups of multiple inline-owned steps
and the separate QuickJS scheduler are outside this amendment's scope.

### Local amendment for upstream: a 1ms stream group-commit window

The default leading-chunk window is now 1ms. This lets adjacent events share a
World write instead of sending their first chunk immediately. Explicit environment
and World overrides retain their precedence, including `0` for immediate dispatch.
Writer release, close and durability barriers still drain pending chunks and
propagate write failures.

This trades up to 1ms of isolated-chunk latency for fewer adjacent-event writes.
Local turn measurements selected 1ms over 5ms and 10ms; hosted timing is measured
separately. Keep or re-evaluate this default when adopting an upstream release.

## PostgreSQL hook ownership

`@workflow__world-postgres@5.0.0-beta.39.patch` fixes concurrent hook claims in the
PostgreSQL World. Its token index is nonunique: two callers can both observe no
owner and insert different hook IDs for the same token. The original hook and
creation-event inserts also commit separately, making a hook visible before its
creation event and leaving orphan claims after failed event writes.

The patch takes a transaction-scoped advisory lock for the token, reads retained
ownership in a separate `READ COMMITTED` statement, and writes the hook and event
in the same transaction. Existing hook rows are locked against disposal. Replay
deduplication, orphan recovery, and terminal-token retention keep their existing
semantics. Correlated-event conflicts share the existing error translation.

The uncontended claim uses nine SQL statements, compared with six before the fix:
the added statements are `BEGIN`, the token lock, and `COMMIT`. This cost applies
only to PostgreSQL claims. No eve preflight or Vercel World request is added.

The [PostgreSQL integration suite](./tests/README.md) runs the installed package
against its actual migrations. The unpatched implementation reproduced duplicate
owners; the patched implementation passes concurrent claims, replay, failed-write
rollback, atomic publication, disposal, and retention checks. All writers using a
database must use the fix; this patch does not repair already-duplicated tokens.
Remove the patch and its `patchedDependencies` entry when upgrading to an upstream
release with equivalent atomic ownership guarantees.
