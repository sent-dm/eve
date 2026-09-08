import { parseNdjsonStream } from "#execution/ndjson-stream.js";
import type { EventStreamRef } from "#execution/session/resources.js";
import {
  createStreamStorageScope,
  type StreamStorageScope,
} from "#execution/session/stream-storage.js";
import { encodeMessageStreamEvent, type MessageStreamEvent } from "#protocol/message.js";

export const sessionEvents = {
  open(ref: EventStreamRef, scope: StreamStorageScope = createStreamStorageScope()) {
    const storage = scope.open(ref.id);
    return {
      append(events: readonly MessageStreamEvent[]): Promise<void> {
        return storage.append(events.map(encodeMessageStreamEvent));
      },
      read(options?: { readonly startIndex?: number }): ReadableStream<MessageStreamEvent> {
        return parseNdjsonStream(() => storage.read<Uint8Array>(options?.startIndex));
      },
      tailIndex: storage.tailIndex,
      withWriter<Result>(
        run: (writable: WritableStream<Uint8Array>) => Promise<Result>,
      ): Promise<Result> {
        return storage.withWriter(run);
      },
      close(): Promise<void> {
        return storage.append([], true);
      },
    };
  },

  read(
    ref: EventStreamRef,
    options?: { readonly startIndex?: number },
  ): ReadableStream<MessageStreamEvent> {
    return sessionEvents.open(ref).read(options);
  },

  tailIndex(ref: EventStreamRef): Promise<number> {
    return sessionEvents.open(ref).tailIndex();
  },
};
