import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(new URL("../../packages/eve/package.json", import.meta.url));
const sdk = dirname(dirname(require.resolve("@workflow/core/runtime/run")));

export default {
  root: fileURLToPath(new URL("../..", import.meta.url)),
  resolve: {
    alias: {
      "#workflow-sdk": sdk,
      "@workflow/serde": require.resolve("@workflow/serde"),
      vitest: join(dirname(require.resolve("vitest/package.json")), "dist/index.js"),
    },
  },
  test: {
    // Transform the SDK so World mocks also apply to its internal imports.
    server: { deps: { inline: [/\/@workflow\/core\//] } },
    include: ["patches/tests/workflow-run-stream-target.test.ts"],
    testTimeout: 5_000,
    fileParallelism: false,
  },
};
