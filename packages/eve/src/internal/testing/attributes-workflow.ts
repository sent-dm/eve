import { getStepMetadata, getWorkflowMetadata } from "#compiled/@workflow/core/index.js";
import { setEveAttributes } from "#runtime/attributes/emit.js";

export async function attributesFixtureWorkflow(): Promise<{ runId: string; stepId: string }> {
  "use workflow";
  return await writeAttributesFixtureStep();
}

async function writeAttributesFixtureStep(): Promise<{ runId: string; stepId: string }> {
  "use step";
  setEveAttributes({ "$eve.type": "turn" });
  return { runId: getWorkflowMetadata().workflowRunId, stepId: getStepMetadata().stepId };
}
