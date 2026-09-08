import type { EveEvalContext } from "eve/evals";
import { respondWhileRunning, waitForInput } from "./live-input.shared.ts";

export type ProbeCase = { readonly kind: "auth" | "hitl" };

export async function runProbe(t: EveEvalContext, probe: ProbeCase): Promise<void> {
  const live = await t.start(`WORKFLOW-PROBE-blocking-local-${probe.kind}`);
  if (probe.kind === "hitl") {
    const request = await waitForInput(t, live, { toolName: "approval-gate" });
    const approved = await respondWhileRunning(t, live, request, "approve");
    approved.expectOk();
    approved.messageIncludes("WORKFLOW-HITL:approved");
  } else {
    const required = await live.waitForEvent("authorization.required");
    const url = required.data.authorization?.url;
    if (url === undefined) throw new Error("Authorization probe produced no callback URL.");
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Authorization callback failed (${response.status}).`);
    await live.waitForEvent("authorization.completed");
    const authorized = await live.result();
    authorized.expectOk();
    authorized.messageIncludes("WORKFLOW-AUTH:authorized");
  }
  t.succeeded();
  t.noFailedActions();
}
