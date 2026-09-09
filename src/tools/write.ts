import { z } from "zod";
import type { ToolDef } from "./types.js";
import { jsonResult, textResult } from "./types.js";

export const writeTools: ToolDef[] = [
  {
    name: "create_topic",
    capability: "write",
    config: {
      title: "Create topic",
      description: "Create a topic with a partition count, replication factor, and optional configs.",
      inputSchema: {
        topic: z.string().describe("Topic name"),
        numPartitions: z.number().int().min(1).optional().describe("Partition count (default 1)"),
        replicationFactor: z.number().int().min(1).optional().describe("Replication factor (default 1)"),
        configs: z
          .record(z.string())
          .optional()
          .describe("Topic configs, e.g. { \"retention.ms\": \"604800000\" }"),
      },
    },
    handler: async (args, { client, policy }) => {
      const topic = args.topic as string;
      const { dryRun } = policy.guard({ tool: "create_topic", capability: "write", topic });
      const configEntries = Object.entries((args.configs as Record<string, string> | undefined) ?? {}).map(
        ([name, value]) => ({ name, value }),
      );
      const spec = {
        topic,
        numPartitions: (args.numPartitions as number | undefined) ?? 1,
        replicationFactor: (args.replicationFactor as number | undefined) ?? 1,
        configEntries,
      };
      if (dryRun) return textResult(`[dry-run] Would create topic: ${JSON.stringify(spec)}`);
      const created = await client.createTopic(spec);
      return jsonResult({ created, topic, alreadyExisted: !created });
    },
  },
  {
    name: "create_partitions",
    capability: "write",
    config: {
      title: "Add partitions",
      description: "Increase a topic's partition count (cannot decrease). Affects key-based ordering.",
      inputSchema: {
        topic: z.string().describe("Topic name"),
        count: z.number().int().min(1).describe("New TOTAL partition count (must exceed current)"),
      },
    },
    handler: async (args, { client, policy }) => {
      const topic = args.topic as string;
      const count = args.count as number;
      const { dryRun } = policy.guard({ tool: "create_partitions", capability: "write", topic });
      if (dryRun) return textResult(`[dry-run] Would set topic '${topic}' to ${count} partitions.`);
      await client.createPartitions(topic, count);
      return jsonResult({ updated: true, topic, partitions: count });
    },
  },
  {
    name: "alter_topic_config",
    capability: "write",
    config: {
      title: "Alter topic config",
      description: "Update one or more topic-level configuration entries.",
      inputSchema: {
        topic: z.string().describe("Topic name"),
        configs: z.record(z.string()).describe("Config entries to set, e.g. { \"retention.ms\": \"86400000\" }"),
      },
    },
    handler: async (args, { client, policy }) => {
      const topic = args.topic as string;
      const { dryRun } = policy.guard({ tool: "alter_topic_config", capability: "write", topic });
      const entries = Object.entries(args.configs as Record<string, string>).map(([name, value]) => ({
        name,
        value,
      }));
      if (dryRun) return textResult(`[dry-run] Would set ${topic} configs: ${JSON.stringify(entries)}`);
      await client.alterTopicConfig(topic, entries);
      return jsonResult({ updated: true, topic, entries });
    },
  },
  {
    name: "reset_consumer_group_offsets",
    capability: "write",
    config: {
      title: "Reset consumer group offsets",
      description:
        "Reset a consumer group's committed offsets for a topic to the earliest or latest offset. " +
        "The group must have no active members. Affects redelivery — use with care.",
      inputSchema: {
        groupId: z.string().describe("Consumer group id"),
        topic: z.string().describe("Topic name"),
        to: z.enum(["earliest", "latest"]).describe("Reset target"),
      },
    },
    handler: async (args, { client, policy, confirm }) => {
      const topic = args.topic as string;
      const groupId = args.groupId as string;
      const earliest = (args.to as string) === "earliest";
      const { dryRun } = policy.guard({
        tool: "reset_consumer_group_offsets",
        capability: "write",
        topic,
      });
      if (dryRun)
        return textResult(`[dry-run] Would reset group '${groupId}' on '${topic}' to ${args.to}.`);
      const ok = await confirm.confirm({ action: "reset consumer group offsets (affects redelivery)", target: groupId, details: { topic, to: args.to as string } });
      if (!ok.approved) return textResult(`Reset cancelled — ${ok.reason}.`);
      await client.resetGroupOffsets(groupId, topic, earliest);
      return jsonResult({ reset: true, groupId, topic, to: args.to });
    },
  },
];
