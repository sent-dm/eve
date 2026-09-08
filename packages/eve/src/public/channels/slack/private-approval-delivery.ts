import { createLogger, logError } from "#internal/logging.js";
import { renderInputRequestPostParts, type SlackHitlRoute } from "#public/channels/slack/hitl.js";
import type { SlackEventContext } from "#public/channels/slack/slackChannel.js";
import type { InputRequest } from "#shared/input.js";

const log = createLogger("slack.private-approval-delivery");

export async function deliverPrivateToolApproval(input: {
  readonly channel: SlackEventContext;
  readonly previewMessageTs: string;
  readonly request: InputRequest;
  readonly reviewer: string;
}): Promise<void> {
  const open = await input.channel.slack.request("conversations.open", { users: input.reviewer });
  const messageChannelId =
    open.ok === true ? (open.channel as { id?: unknown } | undefined)?.id : undefined;
  if (typeof messageChannelId !== "string" || messageChannelId.length === 0) {
    throw new Error(`Slack conversations.open failed: ${open.error ?? "unknown_error"}`);
  }

  const route: SlackHitlRoute = {
    channelId: input.channel.slack.channelId,
    threadTs: input.channel.slack.threadTs,
  };
  const parts = renderInputRequestPostParts(input.request, route);
  const postedMessageIds: string[] = [];

  try {
    const permalink = await resolveMessagePermalink(input.channel, input.previewMessageTs);
    if (permalink !== undefined) {
      postedMessageIds.push(
        await postMessage(input.channel, {
          channel: messageChannelId,
          markdown_text: permalink,
          unfurl_links: true,
          unfurl_media: false,
        }),
      );
    }
    if (parts.details !== undefined) {
      postedMessageIds.push(
        await postMessage(input.channel, {
          blocks: parts.details.blocks,
          channel: messageChannelId,
          text: parts.details.text,
          unfurl_links: false,
          unfurl_media: false,
        }),
      );
    }
    const messageTs = await postMessage(input.channel, {
      blocks: parts.controls.blocks,
      channel: messageChannelId,
      text: parts.controls.text,
      unfurl_links: false,
      unfurl_media: false,
    });
    postedMessageIds.push(messageTs);
    input.channel.state.pendingApprovalCards = {
      ...input.channel.state.pendingApprovalCards,
      [input.request.requestId]: {
        messageBlocks: parts.controls.blocks,
        messageChannelId,
        messageTs,
      },
    };
  } catch (error) {
    await Promise.allSettled(
      postedMessageIds.map(async (ts) => {
        try {
          await input.channel.slack.request("chat.delete", { channel: messageChannelId, ts });
        } catch (cleanupError) {
          logError(log, "failed to roll back partial private approval delivery", cleanupError, {
            channelId: messageChannelId,
            messageTs: ts,
          });
        }
      }),
    );
    throw error;
  }
}

async function resolveMessagePermalink(
  channel: SlackEventContext,
  messageTs: string,
): Promise<string | undefined> {
  if (!channel.slack.channelId || !messageTs) return undefined;
  const response = await channel.slack.request("chat.getPermalink", {
    channel: channel.slack.channelId,
    message_ts: messageTs,
  });
  return response.ok === true && typeof response.permalink === "string"
    ? response.permalink
    : undefined;
}

async function postMessage(
  channel: SlackEventContext,
  body: Record<string, unknown>,
): Promise<string> {
  const response = await channel.slack.request("chat.postMessage", body);
  if (response.ok !== true || typeof response.ts !== "string" || response.ts.length === 0) {
    throw new Error(`Slack chat.postMessage failed: ${response.error ?? "unknown_error"}`);
  }
  return response.ts;
}
