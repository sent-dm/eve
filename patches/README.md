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

The patch applies the changes to `src/runtime/run.ts`, `src/runtime.ts`, and
`src/serialization.ts` onto the pinned package's sources. JavaScript, declarations,
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
