/**
 * Built-in Workflow response body steps used by eve-owned workflow bundles.
 *
 * These mirror Workflow's tiny `workflow/internal/builtins` module without
 * requiring eve to depend on Workflow's umbrella package.
 */
export async function __builtin_response_array_buffer(
  this: Request | Response,
): Promise<ArrayBuffer> {
  "use step";
  return await this.arrayBuffer();
}

export async function __builtin_response_json(this: Request | Response): Promise<unknown> {
  "use step";
  return await this.json();
}

export async function __builtin_response_text(this: Request | Response): Promise<string> {
  "use step";
  return await this.text();
}
