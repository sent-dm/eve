import { z } from "zod";

import { defineDynamic, defineTool } from "#public/tools/index.js";

// Epoch 31 resolvers never depended on the removed internal Workflow session metadata.
export default defineDynamic({
  events: {
    "turn.started": (_event, ctx) => {
      const sessionId = ctx.session.id;
      return defineTool({
        description: "Report the current session and supplied label.",
        inputSchema: z.object({ label: z.string() }),
        label: { start: ({ label }) => `Report ${label}` },
        execute: ({ label }) => ({ label, sessionId }),
      });
    },
  },
});
