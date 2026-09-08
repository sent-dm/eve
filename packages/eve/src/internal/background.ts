import { waitUntil } from "#compiled/@vercel/functions/index.js";

/** Starts best-effort host work whose result cannot affect the request or workflow step. */
export function runInBackground(task: Promise<unknown>, onError: (error: unknown) => void): void {
  const report = (error: unknown): void => {
    try {
      onError(error);
    } catch {
      // An error reporter must not turn detached work into an unhandled rejection.
    }
  };
  const settled = task.catch(report);
  try {
    waitUntil(settled);
  } catch (error) {
    report(error);
  }
}
