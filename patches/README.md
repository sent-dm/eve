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
