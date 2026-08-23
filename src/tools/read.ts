import { z } from "zod";
import type { ToolDef } from "./types.js";
import { jsonResult } from "./types.js";

export const readTools: ToolDef[] = [
  {
    name: "cluster_info",
    capability: "read",
    config: {
      title: "Cluster info",
      description: "Describe the Kafka cluster: brokers, controller, and cluster id.",
      inputSchema: {},
    },
    handler: async (_args, { client, policy }) => {
      policy.guard({ tool: "cluster_info", capability: "read" });
      return jsonResult(await client.clusterInfo());
    },
  },
  {
    name: "list_topics",
    capability: "read",
    config: {
      title: "List topics",
      description: "List topic names. Topics outside the allowlist are filtered out.",
      inputSchema: {
        includeInternal: z.boolean().optional().describe("Include internal topics (starting with _)"),
      },
    },
    handler: async (args, { client, policy }) => {
      policy.guard({ tool: "list_topics", capability: "read" });
      const includeInternal = (args.includeInternal as boolean | undefined) ?? false;
      const topics = (await client.listTopics())
        .filter((t) => policy.isTopicAllowed(t))
        .filter((t) => includeInternal || !t.startsWith("_"))
        .sort();
      return jsonResult(topics);
    },
  },
  {
    name: "describe_topic",
    capability: "read",
    config: {
      title: "Describe topic",
      description: "Partitions, replicas, in-sync replicas, and non-default configs for a topic.",
      inputSchema: { topic: z.string().describe("Topic name") },
    },
    handler: async (args, { client, policy }) => {
      const topic = args.topic as string;
      policy.guard({ tool: "describe_topic", capability: "read", topic });
      return jsonResult(await client.describeTopic(topic));
    },
  },
  {
    name: "topic_offsets",
    capability: "read",
    config: {
      title: "Topic offsets",
      description: "Earliest and latest offsets per partition for a topic (message backlog view).",
      inputSchema: { topic: z.string().describe("Topic name") },
    },
    handler: async (args, { client, policy }) => {
      const topic = args.topic as string;
      policy.guard({ tool: "topic_offsets", capability: "read", topic });
      return jsonResult(await client.topicOffsets(topic));
    },
  },
  {
    name: "list_consumer_groups",
    capability: "read",
    config: {
      title: "List consumer groups",
      description: "List consumer groups and their protocol types.",
      inputSchema: {},
    },
    handler: async (_args, { client, policy }) => {
      policy.guard({ tool: "list_consumer_groups", capability: "read" });
      return jsonResult(await client.listGroups());
    },
  },
  {
    name: "describe_consumer_group",
    capability: "read",
    config: {
      title: "Describe consumer group (with lag)",
      description:
        "Describe a consumer group's state and compute per-partition and total lag across its topics.",
      inputSchema: { groupId: z.string().describe("Consumer group id") },
    },
    handler: async (args, { client, policy }) => {
      policy.guard({ tool: "describe_consumer_group", capability: "read" });
      return jsonResult(await client.describeGroupLag(args.groupId as string));
    },
  },
];
