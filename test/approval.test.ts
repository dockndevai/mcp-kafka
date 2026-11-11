import { afterEach, describe, expect, it, vi } from "vitest";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { ElicitRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import type { AppConfig } from "../src/config.js";
import { KafkaClient } from "../src/kafka/client.js";
import { buildServer } from "../src/server.js";

const config = (requireHumanApproval = false): AppConfig => ({
  connection: { brokers: ["localhost:9092"], clientId: "t", ssl: false, connectionTimeout: 1000 } as AppConfig["connection"],
  security: {
    mode: "admin",
    topicAllowlist: [],
    protectedTopics: [],
    protectInternalTopics: true,
    allowDelete: true,
    dryRun: false,
    auditLog: false,
    requireHumanApproval,
  },
});

type Answer = { action: "accept" | "decline"; content?: Record<string, unknown> };

/** Connect an in-memory MCP client (optionally able to answer elicitation) to a freshly built server. */
async function connect(opts: { requireHuman?: boolean; answer?: (message: string) => Answer | Promise<Answer>; elicits?: boolean }) {
  const { server } = buildServer(config(opts.requireHuman));
  const prompts: string[] = [];
  const client = new Client({ name: "t", version: "0" }, { capabilities: opts.elicits === false ? {} : { elicitation: {} } });
  if (opts.elicits !== false) {
    client.setRequestHandler(ElicitRequestSchema, async (req) => {
      prompts.push(req.params.message);
      return (await opts.answer?.(req.params.message)) ?? { action: "accept", content: { confirm: true, typed: "orders" } };
    });
  }
  const [a, b] = InMemoryTransport.createLinkedPair();
  await Promise.all([server.connect(a), client.connect(b)]);
  return { client, prompts };
}

const text = (r: unknown) => ((r as { content: Array<{ text: string }> }).content[0]?.text ?? "");

afterEach(() => vi.restoreAllMocks());

describe("delete_topic approval (end to end)", () => {
  it("shows the tool, full arguments, version and fingerprint, then deletes", async () => {
    vi.spyOn(KafkaClient.prototype, "topicVersion").mockResolvedValue("v1");
    const del = vi.spyOn(KafkaClient.prototype, "deleteTopic").mockResolvedValue();
    const { client, prompts } = await connect({});
    const res = await client.callTool({ name: "delete_topic", arguments: { topic: "orders" } });
    expect(text(res)).toContain('"deleted": true');
    expect(del).toHaveBeenCalledWith("orders");
    expect(prompts[0]).toContain("tool: delete_topic");
    expect(prompts[0]).toContain('{"topic":"orders"}');
    expect(prompts[0]).toContain("resource version: v1");
    expect(prompts[0]).toMatch(/fingerprint: [0-9a-f]{16}/);
  });

  it("does NOT delete when the topic changed while the human was deciding", async () => {
    const versions = ["v1", "v2"];
    vi.spyOn(KafkaClient.prototype, "topicVersion").mockImplementation(async () => versions.shift());
    const del = vi.spyOn(KafkaClient.prototype, "deleteTopic").mockResolvedValue();
    const { client } = await connect({});
    const res = await client.callTool({ name: "delete_topic", arguments: { topic: "orders" } });
    expect(text(res)).toContain("resource changed since the prompt");
    expect(del).not.toHaveBeenCalled();
  });

  it("does NOT delete when the typed confirmation does not match", async () => {
    vi.spyOn(KafkaClient.prototype, "topicVersion").mockResolvedValue("v1");
    const del = vi.spyOn(KafkaClient.prototype, "deleteTopic").mockResolvedValue();
    const { client } = await connect({ answer: () => ({ action: "accept", content: { confirm: true, typed: "wrong" } }) });
    const res = await client.callTool({ name: "delete_topic", arguments: { topic: "orders" } });
    expect(text(res)).toContain("typed confirmation did not match");
    expect(del).not.toHaveBeenCalled();
  });

  it("does NOT delete when the human declines", async () => {
    vi.spyOn(KafkaClient.prototype, "topicVersion").mockResolvedValue("v1");
    const del = vi.spyOn(KafkaClient.prototype, "deleteTopic").mockResolvedValue();
    const { client } = await connect({ answer: () => ({ action: "decline" }) });
    await client.callTool({ name: "delete_topic", arguments: { topic: "orders" } });
    expect(del).not.toHaveBeenCalled();
  });
});

describe("KAFKA_REQUIRE_HUMAN_APPROVAL", () => {
  it("falls back to the flags when the client cannot elicit (default)", async () => {
    vi.spyOn(KafkaClient.prototype, "topicVersion").mockResolvedValue("v1");
    const del = vi.spyOn(KafkaClient.prototype, "deleteTopic").mockResolvedValue();
    const { client } = await connect({ elicits: false });
    await client.callTool({ name: "delete_topic", arguments: { topic: "orders" } });
    expect(del).toHaveBeenCalledWith("orders");
  });

  it("refuses to delete when the client cannot elicit and human approval is required", async () => {
    vi.spyOn(KafkaClient.prototype, "topicVersion").mockResolvedValue("v1");
    const del = vi.spyOn(KafkaClient.prototype, "deleteTopic").mockResolvedValue();
    const { client } = await connect({ elicits: false, requireHuman: true });
    const res = await client.callTool({ name: "delete_topic", arguments: { topic: "orders" } });
    expect(text(res)).toContain("no human approval channel");
    expect(del).not.toHaveBeenCalled();
  });
});

describe("other destructive tools bind their version too", () => {
  it("delete_consumer_group refuses when the group's membership changed", async () => {
    const versions = ["g1", "g2"];
    vi.spyOn(KafkaClient.prototype, "groupVersion").mockImplementation(async () => versions.shift());
    const del = vi.spyOn(KafkaClient.prototype, "deleteGroup").mockResolvedValue([] as never);
    const { client } = await connect({});
    const res = await client.callTool({ name: "delete_consumer_group", arguments: { groupId: "g" } });
    expect(text(res)).toContain("resource changed since the prompt");
    expect(del).not.toHaveBeenCalled();
  });

  it("reset_consumer_group_offsets refuses when committed offsets moved", async () => {
    const versions = ["o1", "o2"];
    vi.spyOn(KafkaClient.prototype, "groupOffsetsVersion").mockImplementation(async () => versions.shift());
    const reset = vi.spyOn(KafkaClient.prototype, "resetGroupOffsets").mockResolvedValue(undefined as never);
    const { client } = await connect({});
    const res = await client.callTool({
      name: "reset_consumer_group_offsets",
      arguments: { groupId: "g", topic: "orders", to: "earliest" },
    });
    expect(text(res)).toContain("resource changed since the prompt");
    expect(reset).not.toHaveBeenCalled();
  });
});
