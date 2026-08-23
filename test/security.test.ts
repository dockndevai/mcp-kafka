import { describe, expect, it } from "vitest";
import { PolicyError, SecurityPolicy, type SecurityConfig } from "../src/security.js";

function makePolicy(overrides: Partial<SecurityConfig> = {}): SecurityPolicy {
  return new SecurityPolicy({
    mode: "read-only",
    topicAllowlist: [],
    protectedTopics: [],
    protectInternalTopics: true,
    allowDelete: false,
    dryRun: false,
    auditLog: false,
    ...overrides,
  });
}

describe("capability gating", () => {
  it("read-only enables read only", () => {
    const p = makePolicy();
    expect(p.isCapabilityEnabled("read")).toBe(true);
    expect(p.isCapabilityEnabled("write")).toBe(false);
    expect(p.isCapabilityEnabled("admin")).toBe(false);
  });

  it("read-write enables read+write, not admin", () => {
    const p = makePolicy({ mode: "read-write" });
    expect(p.isCapabilityEnabled("write")).toBe(true);
    expect(p.isCapabilityEnabled("admin")).toBe(false);
  });
});

describe("guard: capability vs mode", () => {
  it("rejects create in read-only", () => {
    const p = makePolicy();
    expect(() => p.guard({ tool: "create_topic", capability: "write", topic: "orders" })).toThrow(
      PolicyError,
    );
  });
});

describe("topic allowlist", () => {
  it("blocks topics outside a non-empty allowlist", () => {
    const p = makePolicy({ mode: "read-write", topicAllowlist: ["orders"] });
    expect(() => p.guard({ tool: "describe_topic", capability: "read", topic: "payments" })).toThrow(
      /allowlist/,
    );
    expect(() => p.guard({ tool: "describe_topic", capability: "read", topic: "orders" })).not.toThrow();
  });
});

describe("protected / internal topics", () => {
  it("treats internal topics (starting with _) as protected for writes", () => {
    const p = makePolicy({ mode: "admin", allowDelete: true });
    expect(() =>
      p.guard({ tool: "delete_topic", capability: "admin", topic: "__consumer_offsets", destructive: true }),
    ).toThrow(/protected/);
  });

  it("allows reading an internal topic", () => {
    const p = makePolicy({ mode: "admin" });
    expect(() =>
      p.guard({ tool: "describe_topic", capability: "read", topic: "__consumer_offsets" }),
    ).not.toThrow();
  });

  it("honours explicit protected topics", () => {
    const p = makePolicy({ mode: "read-write", protectedTopics: ["orders"], protectInternalTopics: false });
    expect(() =>
      p.guard({ tool: "alter_topic_config", capability: "write", topic: "orders" }),
    ).toThrow(/protected/);
  });
});

describe("destructive gating", () => {
  it("blocks delete without allowDelete", () => {
    const p = makePolicy({ mode: "admin" });
    expect(() =>
      p.guard({ tool: "delete_topic", capability: "admin", topic: "orders", destructive: true }),
    ).toThrow(/ALLOW_DELETE/);
  });

  it("permits delete when allowDelete is true", () => {
    const p = makePolicy({ mode: "admin", allowDelete: true });
    expect(() =>
      p.guard({ tool: "delete_topic", capability: "admin", topic: "orders", destructive: true }),
    ).not.toThrow();
  });
});

describe("dry run", () => {
  it("flags writes but not reads", () => {
    const p = makePolicy({ mode: "read-write", dryRun: true });
    expect(p.guard({ tool: "describe_topic", capability: "read", topic: "orders" }).dryRun).toBe(false);
    expect(p.guard({ tool: "create_topic", capability: "write", topic: "orders" }).dryRun).toBe(true);
  });
});
