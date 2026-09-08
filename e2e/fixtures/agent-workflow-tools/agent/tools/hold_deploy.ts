import { defineTool } from "eve/tools";
import { ask } from "eve/workflow";
import { z } from "zod";

/**
 * Holds an admitted workflow body on a cancellation-aware input wait.
 * Cancelling must unwind this body before its owner can release the turn.
 */
export default defineTool({
  description: "Hold a deploy open until cancelled.",
  inputSchema: z.strictObject({ service: z.string() }),
  async execute({ service }, ctx) {
    "use workflow";
    await ask(ctx, {
      display: "confirmation",
      prompt: `Continue holding ${service}?`,
      options: [{ id: "continue", label: "Continue" }],
    });
    return { held: service };
  },
});
