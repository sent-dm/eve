import { walkCauseChain } from "#shared/errors.js";
import { isObject } from "#shared/guards.js";

export class SessionStorageUnavailableError extends Error {
  override readonly name = "SessionStorageUnavailableError";

  constructor(cause: unknown) {
    super("Session storage is unavailable.", { cause });
  }
}

/** Workflow replay preserves error names, but not the originating prototype. */
export function isSessionStorageUnavailable(error: unknown): boolean {
  return [...walkCauseChain(error)].some(
    (cause) => isObject(cause) && cause.name === "SessionStorageUnavailableError",
  );
}
