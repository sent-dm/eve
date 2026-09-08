import { runInBackground } from "#internal/background.js";
import { normalizeEveAttributes, type EveAttributeValue } from "#runtime/attributes/normalize.js";

export {
  EVE_ATTRIBUTE_VALUE_MAX_BYTES,
  type EveAttributeValue,
  truncateForTag,
} from "#runtime/attributes/normalize.js";

let WARNED_ABOUT_TAG_FAILURE = false;

/**
 * Starts a best-effort observability write from the current workflow step.
 *
 * Reserved-namespace contract:
 * - All keys must use the `$eve.` prefix (the workflow runtime would
 *   otherwise reject them as user-space writes into the reserved `$`
 *   namespace).
 * - The call always opts in via `{ allowReservedAttributes: true }`
 *   on behalf of the framework — authored code never calls this helper
 *   directly.
 *
 * Value normalization:
 * - `undefined` entries are dropped so callers can build attribute
 *   maps with optional fields (`$eve.subagent` is only present on
 *   subagent roots, for example).
 * - Numbers and booleans are stringified (the runtime stores all values as strings).
 * - Strings are truncated to {@link EVE_ATTRIBUTE_VALUE_MAX_BYTES} via
 *   {@link truncateForTag} so a long free-form value (e.g. `$eve.title`)
 *   can never trip the runtime's per-value byte budget.
 *
 * The asynchronous write retains the calling step's context and runs under
 * the host's waitUntil lifetime. Session progress never waits for it. Writes
 * may be dropped if the run completes first, and are not authoritative state.
 * Authorization attributes remain in the atomic workflow start payload.
 */
export function setEveAttributes(attrs: Record<string, EveAttributeValue>): void {
  runInBackground(writeAttributes(attrs), (error) => {
    if (isTerminalRunAttributeError(error) || WARNED_ABOUT_TAG_FAILURE) return;
    WARNED_ABOUT_TAG_FAILURE = true;
    console.warn("[eve] setEveAttributes failed; suppressing further warnings this process.", {
      keys: Object.keys(attrs),
      error: error instanceof Error ? error.message : String(error),
    });
  });
}

async function writeAttributes(attrs: Record<string, EveAttributeValue>): Promise<void> {
  const normalized = normalizeEveAttributes(attrs);

  if (Object.keys(normalized).length === 0) {
    return;
  }

  // Keep core behind its existing dynamic bundle boundary. Async context is
  // inherited here, before the step can return or another step can start.
  const { setAttributes } = await import("#compiled/@workflow/core/index.js");
  await setAttributes(normalized, { allowReservedAttributes: true });
}

function isTerminalRunAttributeError(error: unknown): boolean {
  return (
    error instanceof Error &&
    error.message.startsWith("Cannot set attributes on run in terminal state")
  );
}
