import { fileURLToPath } from "node:url";
import { loadDeclaration } from "../_shared.mjs";

export default {
  packageName: "@vercel/functions",
  compiledPath: "@vercel/functions",
  chunkGroup: "workflow",
  entries: [
    {
      input: fileURLToPath(new URL("../entries/@vercel/functions.mjs", import.meta.url)),
      outputPath: "index",
      declaration: await loadDeclaration("@vercel/functions.d.ts"),
    },
  ],
};
