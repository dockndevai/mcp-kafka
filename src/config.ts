/**
 * Configuration from environment variables.
 */
import type { AccessMode, SecurityConfig } from "./security.js";

export interface KafkaConnection {
  brokers: string[];
  clientId: string;
  ssl: boolean;
  sasl?: {
    mechanism: "plain" | "scram-sha-256" | "scram-sha-512";
    username: string;
    password: string;
  };
  /** Connection/request timeout in ms. */
  connectionTimeout: number;
}

export interface AppConfig {
  connection: KafkaConnection;
  security: SecurityConfig;
}

function bool(name: string, fallback: boolean): boolean {
  const v = process.env[name];
  if (v === undefined || v === "") return fallback;
  return ["1", "true", "yes", "on"].includes(v.toLowerCase());
}

function list(name: string): string[] {
  const v = process.env[name];
  if (!v) return [];
  return v
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function parseMode(): AccessMode {
  const raw = (process.env.KAFKA_MODE ?? "read-only").toLowerCase();
  if (raw === "read-only" || raw === "read-write" || raw === "admin") return raw;
  throw new Error(`Invalid KAFKA_MODE '${raw}'. Expected one of: read-only, read-write, admin.`);
}

function parseSasl(): KafkaConnection["sasl"] {
  const mechanism = process.env.KAFKA_SASL_MECHANISM?.toLowerCase();
  if (!mechanism) return undefined;
  if (!["plain", "scram-sha-256", "scram-sha-512"].includes(mechanism)) {
    throw new Error(
      `Invalid KAFKA_SASL_MECHANISM '${mechanism}'. Expected plain, scram-sha-256, or scram-sha-512.`,
    );
  }
  const username = process.env.KAFKA_SASL_USERNAME;
  const password = process.env.KAFKA_SASL_PASSWORD;
  if (!username || !password) {
    throw new Error("KAFKA_SASL_MECHANISM is set but KAFKA_SASL_USERNAME/PASSWORD are missing.");
  }
  return { mechanism: mechanism as "plain" | "scram-sha-256" | "scram-sha-512", username, password };
}

export function loadConfig(): AppConfig {
  const brokers = list("KAFKA_BROKERS");
  if (brokers.length === 0) {
    throw new Error("Missing required environment variable: KAFKA_BROKERS (comma-separated host:port list).");
  }
  return {
    connection: {
      brokers,
      clientId: process.env.KAFKA_CLIENT_ID || "mcp-kafka",
      ssl: bool("KAFKA_SSL", false),
      sasl: parseSasl(),
      connectionTimeout: Number(process.env.KAFKA_CONNECTION_TIMEOUT_MS ?? 10000),
    },
    security: {
      mode: parseMode(),
      topicAllowlist: list("KAFKA_TOPIC_ALLOWLIST"),
      protectedTopics: list("KAFKA_PROTECTED_TOPICS"),
      protectInternalTopics: bool("KAFKA_PROTECT_INTERNAL_TOPICS", true),
      allowDelete: bool("KAFKA_ALLOW_DELETE", false),
      dryRun: bool("KAFKA_DRY_RUN", false),
      auditLog: bool("KAFKA_AUDIT_LOG", true),
    },
  };
}
