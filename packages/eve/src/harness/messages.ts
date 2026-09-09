import type { ModelMessage, TextPart, UserContent } from "ai";

import type { InputResponse } from "#shared/input.js";
import type { StepInput } from "#harness/types.js";
import { attachClientContext, readClientContext } from "#internal/client-context.js";

/**
 * Merges two {@link StepInput} values into one.
 *
 * Used by the harness to coalesce deferred step input with the current
 * turn's input, and by the execution layer after calling `onDeliver`
 * for each payload within a delivery.
 */
export function coalesceTurnInputs(a: StepInput, b: StepInput): StepInput {
  const inputResponses = coalesceInputResponses({
    a: a.inputResponses,
    b: b.inputResponses,
  });
  const message = coalesceMessage({
    a: a.message,
    b: b.message,
  });
  const context = coalesceContext({
    a: a.context,
    b: b.context,
  });
  const ephemeralContext = coalesceContext({
    a: readClientContext(a),
    b: readClientContext(b),
  });
  const outputSchema = b.outputSchema ?? a.outputSchema;

  const result: {
    inputResponses?: readonly InputResponse[];
    message?: string | UserContent;
    context?: readonly string[];
    outputSchema?: StepInput["outputSchema"];
  } = {};

  if (inputResponses !== undefined) {
    result.inputResponses = inputResponses;
  }

  if (message !== undefined) {
    result.message = message;
  }

  if (context !== undefined) {
    result.context = context;
  }

  if (outputSchema !== undefined) {
    result.outputSchema = outputSchema;
  }

  return attachClientContext(result, ephemeralContext);
}

/**
 * Removes text parts with no model-visible content from a user message.
 *
 * Returns `undefined` when no parts remain, allowing callers to omit the user
 * turn entirely rather than create an empty model prompt block.
 */
export function normalizeUserContent(
  content: string | UserContent | undefined,
): string | UserContent | undefined {
  if (content === undefined) {
    return undefined;
  }

  if (typeof content === "string") {
    return content.trim().length > 0 ? content : undefined;
  }

  const parts = content.filter((part) => part.type !== "text" || part.text.trim().length > 0);
  if (parts.length === 0) {
    return undefined;
  }
  return parts.length === content.length ? content : parts;
}

/** Removes blank text blocks that some providers reject from model-bound history. */
export function normalizeModelMessages(messages: readonly ModelMessage[]): ModelMessage[] {
  return messages.flatMap((message) => {
    if (typeof message.content === "string") {
      return message.content.trim().length > 0 ? [message] : [];
    }

    const content = message.content.filter(
      (part) => part.type !== "text" || part.text.trim().length > 0,
    );
    if (content.length === 0) return [];
    return content.length === message.content.length
      ? [message]
      : [{ ...message, content } as ModelMessage];
  });
}

/**
 * Extracts the final visible assistant text from model response messages.
 *
 * Prefers text extracted from the last assistant message that contains visible
 * text. Falls back to the raw `text` property from the AI SDK result when no
 * assistant message contains text. Returns `null` when neither source contains
 * text.
 */
export function resolveAssistantStepText(
  messages: readonly ModelMessage[],
  fallback: string | undefined,
): string | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message?.role !== "assistant") {
      continue;
    }

    const text = extractMessageText(message);
    if (text.trim().length > 0) {
      return text;
    }
  }

  if (fallback !== undefined && fallback.trim().length > 0) {
    return fallback;
  }

  return null;
}

function extractMessageText(message: ModelMessage): string {
  if (typeof message.content === "string") {
    return message.content;
  }

  if (!Array.isArray(message.content)) {
    return "";
  }

  return message.content
    .flatMap((part) => {
      if (typeof part === "string") {
        return [part];
      }

      return "type" in part && part.type === "text" && typeof part.text === "string"
        ? [part.text]
        : [];
    })
    .join("");
}

function coalesceInputResponses(input: {
  readonly a?: readonly InputResponse[];
  readonly b?: readonly InputResponse[];
}): readonly InputResponse[] | undefined {
  const a = input.a ?? [];
  const b = input.b ?? [];

  if (a.length === 0 && b.length === 0) {
    return undefined;
  }

  return [...a, ...b];
}

function coalesceContext(input: {
  readonly a?: readonly string[];
  readonly b?: readonly string[];
}): readonly string[] | undefined {
  const a = input.a ?? [];
  const b = input.b ?? [];

  if (a.length === 0 && b.length === 0) {
    return undefined;
  }

  return [...a, ...b];
}

/**
 * Merges two optional turn messages into one after removing blank content.
 */
function coalesceMessage(input: {
  readonly a?: string | UserContent;
  readonly b?: string | UserContent;
}): string | UserContent | undefined {
  const a = normalizeUserContent(input.a);
  const b = normalizeUserContent(input.b);

  if (a === undefined) {
    return b;
  }

  if (b === undefined) {
    return a;
  }

  return appendUserContent({ appended: b, existing: a });
}

/**
 * Appends user content while preserving structured attachment parts.
 */
export function appendUserContent(input: {
  readonly appended: string | UserContent;
  readonly existing: string | UserContent;
}): string | UserContent {
  if (typeof input.existing === "string" && typeof input.appended === "string") {
    return `${input.existing}\n\n${input.appended}`;
  }

  const merged: UserContentArray = [
    ...toUserContentArray(input.existing),
    ...toUserContentArray(input.appended),
  ];
  return merged;
}

type UserContentArray = Exclude<UserContent, string>;

function toUserContentArray(value: string | UserContent): UserContentArray {
  if (typeof value === "string") {
    return value.length > 0 ? [{ type: "text", text: value } satisfies TextPart] : [];
  }
  if (Array.isArray(value)) {
    return [...value];
  }
  return [];
}
