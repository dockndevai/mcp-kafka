/**
 * Security policy engine.
 *
 * Flags decide which tools are registered (capability vs. access mode) and
 * whether each individual call is allowed at runtime (topic scoping, protected
 * topics, destructive-op gating, dry-run). Pure logic — fully unit-testable.
 */

export type Capability = "read" | "write" | "admin";
export type AccessMode = "read-only" | "read-write" | "admin";

const MODE_RANK: Record<AccessMode, number> = {
  "read-only": 0,
  "read-write": 1,
  admin: 2,
};

const CAPABILITY_RANK: Record<Capability, number> = {
  read: 0,
  write: 1,
  admin: 2,
};

export interface SecurityConfig {
  mode: AccessMode;
  /** If set, only these topics may be touched. Empty = all. */
  topicAllowlist: string[];
  /** Topics that can be read but never mutated or deleted. */
  protectedTopics: string[];
  /** Treat any topic starting with "_" (internal topics) as protected. */
  protectInternalTopics: boolean;
  /** Destructive delete_* operations require this to be true. */
  allowDelete: boolean;
  /** Validate + log writes without sending them to the cluster. */
  dryRun: boolean;
  /** Emit a JSON audit line to stderr per guarded operation. */
  auditLog: boolean;
}

export class PolicyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PolicyError";
  }
}

export interface GuardContext {
  tool: string;
  capability: Capability;
  /** Topic the operation targets, if any. */
  topic?: string;
  /** Destructive delete operation — needs allowDelete. */
  destructive?: boolean;
}

export class SecurityPolicy {
  constructor(private readonly config: SecurityConfig) {}

  get mode(): AccessMode {
    return this.config.mode;
  }

  isCapabilityEnabled(capability: Capability): boolean {
    return CAPABILITY_RANK[capability] <= MODE_RANK[this.config.mode];
  }

  isTopicAllowed(topic: string): boolean {
    if (this.config.topicAllowlist.length === 0) return true;
    return this.config.topicAllowlist.includes(topic);
  }

  isTopicProtected(topic: string): boolean {
    if (this.config.protectInternalTopics && topic.startsWith("_")) return true;
    return this.config.protectedTopics.includes(topic);
  }

  guard(ctx: GuardContext): { dryRun: boolean } {
    if (!this.isCapabilityEnabled(ctx.capability)) {
      this.audit(ctx, "DENY", `capability '${ctx.capability}' exceeds mode '${this.config.mode}'`);
      throw new PolicyError(
        `Operation '${ctx.tool}' requires '${ctx.capability}' access but the server runs in '${this.config.mode}' mode.`,
      );
    }

    if (ctx.topic !== undefined) {
      if (!this.isTopicAllowed(ctx.topic)) {
        this.audit(ctx, "DENY", `topic '${ctx.topic}' not in allowlist`);
        throw new PolicyError(
          `Topic '${ctx.topic}' is not in the configured allowlist (KAFKA_TOPIC_ALLOWLIST).`,
        );
      }
      if (ctx.capability !== "read" && this.isTopicProtected(ctx.topic)) {
        this.audit(ctx, "DENY", `topic '${ctx.topic}' is protected`);
        throw new PolicyError(
          `Topic '${ctx.topic}' is protected (internal or KAFKA_PROTECTED_TOPICS); mutations are refused.`,
        );
      }
    }

    if (ctx.destructive && !this.config.allowDelete) {
      this.audit(ctx, "DENY", "delete not enabled");
      throw new PolicyError(
        `Destructive operation '${ctx.tool}' is disabled. Set KAFKA_ALLOW_DELETE=true to enable it.`,
      );
    }

    const dryRun = ctx.capability !== "read" && this.config.dryRun;
    this.audit(ctx, dryRun ? "DRY_RUN" : "ALLOW");
    return { dryRun };
  }

  private audit(ctx: GuardContext, decision: string, reason?: string): void {
    if (!this.config.auditLog) return;
    const line = {
      ts: new Date().toISOString(),
      audit: "kafka-mcp",
      decision,
      tool: ctx.tool,
      capability: ctx.capability,
      topic: ctx.topic ?? null,
      destructive: ctx.destructive ?? false,
      ...(reason ? { reason } : {}),
    };
    process.stderr.write(`${JSON.stringify(line)}\n`);
  }
}
