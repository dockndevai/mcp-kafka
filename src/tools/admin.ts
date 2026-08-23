import { z } from "zod";
import type { ToolDef } from "./types.js";
import { jsonResult, textResult } from "./types.js";

/**
 * Admin tools are the most privileged: only registered in admin mode, and the
 * destructive ones additionally require KAFKA_ALLOW_DELETE=true.
 */
export const adminTools: ToolDef[] = [
  {
    name: "delete_topic",
    capability: "admin",
    config: {
      title: "Delete topic",
      description:
        "Permanently delete a topic and its data. Requires admin mode AND KAFKA_ALLOW_DELETE=true. Irreversible.",
      inputSchema: { topic: z.string().describe("Topic name") },
    },
    handler: async (args, { client, policy }) => {
      const topic = args.topic as string;
      const { dryRun } = policy.guard({
        tool: "delete_topic",
        capability: "admin",
        topic,
        destructive: true,
      });
      if (dryRun) return textResult(`[dry-run] Would delete topic '${topic}'.`);
      await client.deleteTopic(topic);
      return jsonResult({ deleted: true, topic });
    },
  },
  {
    name: "delete_consumer_group",
    capability: "admin",
    config: {
      title: "Delete consumer group",
      description:
        "Delete a consumer group (must have no active members). Requires admin mode AND KAFKA_ALLOW_DELETE=true.",
      inputSchema: { groupId: z.string().describe("Consumer group id") },
    },
    handler: async (args, { client, policy }) => {
      const groupId = args.groupId as string;
      const { dryRun } = policy.guard({
        tool: "delete_consumer_group",
        capability: "admin",
        destructive: true,
      });
      if (dryRun) return textResult(`[dry-run] Would delete consumer group '${groupId}'.`);
      const result = await client.deleteGroup(groupId);
      return jsonResult({ deleted: true, groupId, result });
    },
  },
];
