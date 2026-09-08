import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(new URL("../../packages/eve/package.json", import.meta.url));
const sdk = dirname(dirname(require.resolve("@workflow/core/runtime/run")));

export default {
  root: fileURLToPath(new URL("../..", import.meta.url)),
  resolve: {
    alias: [
      { find: "#workflow-sdk", replacement: sdk },
      { find: /^@workflow\/serde$/, replacement: require.resolve("@workflow/serde") },
      { find: /^@workflow\/errors$/, replacement: require.resolve("@workflow/errors") },
      { find: /^@workflow\/world$/, replacement: require.resolve("@workflow/world") },
      {
        find: /^@workflow\/utils\/get-port$/,
        replacement: require.resolve("@workflow/utils/get-port"),
      },
      { find: /^@vercel\/functions$/, replacement: require.resolve("@vercel/functions") },
      {
        find: /^vitest$/,
        replacement: join(dirname(require.resolve("vitest/package.json")), "dist/index.js"),
      },
    ],
  },
  test: {
    // Transform the SDK so World mocks also apply to its internal imports.
    server: { deps: { inline: [/\/@workflow\/core\//] } },
    include: ["patches/tests/*.test.ts"],
    testTimeout: 5_000,
    fileParallelism: false,
  },
};
