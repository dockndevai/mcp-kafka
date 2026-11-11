/**
 * Wrapper over the kafkajs Admin client. Lazily connects on first use and
 * exposes the monitoring/management operations the tools need, including
 * consumer-group lag computation.
 */
import { createHash } from "node:crypto";
import { Kafka, type Admin, type ITopicConfig, type SASLOptions, logLevel } from "kafkajs";
import type { KafkaConnection } from "../config.js";

export interface PartitionLag {
  partition: number;
  currentOffset: string;
  endOffset: string;
  lag: number;
}

export interface GroupLag {
  groupId: string;
  state?: string;
  totalLag: number;
  topics: Record<string, PartitionLag[]>;
}

export class KafkaClient {
  private readonly kafka: Kafka;
  private admin?: Admin;

  constructor(conn: KafkaConnection) {
    this.kafka = new Kafka({
      clientId: conn.clientId,
      brokers: conn.brokers,
      ssl: conn.ssl,
      // Our config union is structurally equivalent to kafkajs's discriminated union.
      sasl: conn.sasl as SASLOptions | undefined,
      connectionTimeout: conn.connectionTimeout,
      requestTimeout: conn.connectionTimeout,
      // Keep kafkajs quiet so its logs never pollute the stdio MCP channel.
      logLevel: logLevel.NOTHING,
      retry: { retries: 2 },
    });
  }

  private async getAdmin(): Promise<Admin> {
    if (!this.admin) {
      this.admin = this.kafka.admin();
      await this.admin.connect();
    }
    return this.admin;
  }

  async disconnect(): Promise<void> {
    if (this.admin) await this.admin.disconnect();
  }

  async clusterInfo() {
    const admin = await this.getAdmin();
    return admin.describeCluster();
  }

  async listTopics(): Promise<string[]> {
    const admin = await this.getAdmin();
    return admin.listTopics();
  }

  async describeTopic(topic: string) {
    const admin = await this.getAdmin();
    const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
    const configs = await admin
      .describeConfigs({
        includeSynonyms: false,
        resources: [{ type: 2 /* TOPIC */, name: topic }],
      })
      .catch(() => undefined);
    return {
      metadata: metadata.topics[0],
      config: configs?.resources[0]?.configEntries
        ?.filter((e) => !e.isDefault)
        .map((e) => ({ name: e.configName, value: e.configValue })),
    };
  }

  async topicOffsets(topic: string) {
    const admin = await this.getAdmin();
    return admin.fetchTopicOffsets(topic);
  }

  /**
   * Version token for a topic, pinned across an approval prompt: partition layout (ids + replicas)
   * and each partition's earliest offset. kafkajs exposes no topic UUID, so a recreate that yields the
   * same layout on a topic whose earliest offsets were already 0 is not distinguishable.
   */
  async topicVersion(topic: string): Promise<string | undefined> {
    const admin = await this.getAdmin();
    const md = await admin.fetchTopicMetadata({ topics: [topic] });
    const t = md.topics[0];
    if (!t) return undefined;
    const low = new Map((await admin.fetchTopicOffsets(topic)).map((o) => [o.partition, o.low]));
    const layout = t.partitions
      .map((p) => `${p.partitionId}:${[...p.replicas].sort().join(",")}:${low.get(p.partitionId) ?? "?"}`)
      .sort();
    return createHash("sha256").update(layout.join("|")).digest("hex").slice(0, 12);
  }

  /** Version token for a consumer group: its state and member set. */
  async groupVersion(groupId: string): Promise<string | undefined> {
    const admin = await this.getAdmin();
    const g = (await admin.describeGroups([groupId])).groups[0];
    if (!g) return undefined;
    const members = g.members.map((m) => m.memberId).sort();
    return createHash("sha256").update(`${g.state}|${members.join(",")}`).digest("hex").slice(0, 12);
  }

  /** Version token for a group's committed offsets on one topic. */
  async groupOffsetsVersion(groupId: string, topic: string): Promise<string | undefined> {
    const admin = await this.getAdmin();
    const rows = await admin.fetchOffsets({ groupId, topics: [topic] });
    const parts = rows.flatMap((r) => r.partitions.map((p) => `${p.partition}:${p.offset}`)).sort();
    return createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 12);
  }

  async listGroups() {
    const admin = await this.getAdmin();
    const res = await admin.listGroups();
    return res.groups;
  }

  /** Describe a consumer group and compute per-partition + total lag. */
  async describeGroupLag(groupId: string): Promise<GroupLag> {
    const admin = await this.getAdmin();
    const [desc, offsets] = await Promise.all([
      admin.describeGroups([groupId]),
      admin.fetchOffsets({ groupId }),
    ]);
    const state = desc.groups[0]?.state;

    const topics: Record<string, PartitionLag[]> = {};
    let totalLag = 0;
    for (const t of offsets) {
      const endOffsets = await admin.fetchTopicOffsets(t.topic);
      const endByPartition = new Map(endOffsets.map((o) => [o.partition, o.offset]));
      const rows: PartitionLag[] = [];
      for (const p of t.partitions) {
        const end = endByPartition.get(p.partition) ?? "0";
        // Committed offset of -1 means "no committed offset"; treat as lag = full backlog.
        const current = p.offset === "-1" ? "0" : p.offset;
        const lag = Math.max(0, Number(end) - Number(current));
        totalLag += lag;
        rows.push({ partition: p.partition, currentOffset: p.offset, endOffset: end, lag });
      }
      topics[t.topic] = rows;
    }
    return { groupId, state, totalLag, topics };
  }

  // --- Writes ----------------------------------------------------------------

  async createTopic(topic: ITopicConfig): Promise<boolean> {
    const admin = await this.getAdmin();
    return admin.createTopics({ topics: [topic], waitForLeaders: true });
  }

  async createPartitions(topic: string, count: number): Promise<boolean> {
    const admin = await this.getAdmin();
    return admin.createPartitions({ topicPartitions: [{ topic, count }] });
  }

  async alterTopicConfig(topic: string, entries: Array<{ name: string; value: string }>) {
    const admin = await this.getAdmin();
    return admin.alterConfigs({
      validateOnly: false,
      resources: [{ type: 2 /* TOPIC */, name: topic, configEntries: entries }],
    });
  }

  async resetGroupOffsets(groupId: string, topic: string, earliest: boolean) {
    const admin = await this.getAdmin();
    return admin.resetOffsets({ groupId, topic, earliest });
  }

  // --- Destructive -----------------------------------------------------------

  async deleteTopic(topic: string): Promise<void> {
    const admin = await this.getAdmin();
    await admin.deleteTopics({ topics: [topic] });
  }

  async deleteGroup(groupId: string) {
    const admin = await this.getAdmin();
    return admin.deleteGroups([groupId]);
  }
}
