import { defineAgent } from "#public/index.js";

// Epoch 10 predates `experimental.codeMode`; agents authored then keep compiling unchanged.
export default defineAgent({
  description: "Delegate research with only authored tools.",
  defaultTools: false,
  model: "anthropic/claude-sonnet-5",
  experimental: { workflow: { world: "@workflow/world-postgres" } },
  limits: { maxTokenCostUsdPerSession: 5 },
});
