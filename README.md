# mcp-kafka

[![CI](https://github.com/dockndevai/mcp-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/dockndevai/mcp-kafka/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

A [Model Context Protocol](https://modelcontextprotocol.io) server for **Apache Kafka**. It lets an MCP-capable client (Claude Desktop, Claude Code, etc.) **monitor and manage** Kafka clusters — topics, partitions, configs, and consumer groups (including lag) — with behaviour controlled entirely by flags.

Safe by default: it starts read-only, can be scoped to an allowlist of topics, protects internal/critical topics from mutation, and gates destructive operations behind an explicit opt-in.

## Features

- **Monitoring** — cluster/broker info, topic metadata and offsets, consumer groups, and **per-partition + total consumer lag**.
- **Management** — create topics, add partitions, alter topic configs, reset group offsets; delete topics/groups (admin).
- **Access modes** — `read-only` → `read-write` → `admin`, layered so a mode never exposes tools above its level.
- **Security flags** — topic allowlist, protected/internal topics, delete gating, dry-run, and JSON audit logging (see below).
- **Auth** — plaintext, TLS, and SASL (PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512).

## Security model

| Concern | Flag | Default | Effect |
| --- | --- | --- | --- |
| What can the server do? | `KAFKA_MODE` | `read-only` | `read-only` exposes only monitoring; `read-write` adds management; `admin` adds deletes. Tools above the mode are **never registered**. |
| Which topics are in scope? | `KAFKA_TOPIC_ALLOWLIST` | *(all)* | When set, operations on other topics are refused. |
| Protect internal topics | `KAFKA_PROTECT_INTERNAL_TOPICS` | `true` | Topics starting with `_` can be read but never mutated. |
| Protect specific topics | `KAFKA_PROTECTED_TOPICS` | *(none)* | Additional read-only-forever topics. |
| Can it delete? | `KAFKA_ALLOW_DELETE` | `false` | `delete_topic` / `delete_consumer_group` need this **and** admin mode. |
| Preview without touching the cluster | `KAFKA_DRY_RUN` | `false` | Write/admin tools validate + log intent, then return. |
| Audit trail | `KAFKA_AUDIT_LOG` | `true` | Emits a JSON line to stderr per guarded operation. |

## Tools

**Read** (`read-only`+): `cluster_info`, `list_topics`, `describe_topic`, `topic_offsets`, `list_consumer_groups`, `describe_consumer_group` (with lag)

**Write** (`read-write`+): `create_topic`, `create_partitions`, `alter_topic_config`, `reset_consumer_group_offsets`

**Admin** (`admin`): `delete_topic`, `delete_consumer_group` (both need `KAFKA_ALLOW_DELETE`)

## Use with your MCP client

Works with Claude Code, Claude Desktop, Cursor, OpenAI Codex CLI, Windsurf, VS Code (Copilot), and any other MCP client — see **[docs/CLIENTS.md](docs/CLIENTS.md)** for per-client setup.

## Install

```bash
npm install
npm run build
```

## Run with Claude Desktop / Claude Code

```json
{
  "mcpServers": {
    "kafka": {
      "command": "node",
      "args": ["/absolute/path/to/mcp-kafka/dist/index.js"],
      "env": {
        "KAFKA_BROKERS": "broker1:9092,broker2:9092",
        "KAFKA_MODE": "read-only",
        "KAFKA_SSL": "true",
        "KAFKA_SASL_MECHANISM": "scram-sha-512",
        "KAFKA_SASL_USERNAME": "mcp",
        "KAFKA_SASL_PASSWORD": "…"
      }
    }
  }
}
```

### Example prompts

- *"Which consumer groups have the most lag right now?"*
- *"Describe the `orders` topic and show its offsets."*
- *"Create a topic `events` with 6 partitions and 7-day retention."* (needs `read-write`)

## Develop

```bash
npm run dev
npm test
npm run typecheck
```

## Publishing

This server ships a [`server.json`](server.json) for the official MCP registry and an [`mcpName`](package.json) for npm ownership validation. See **[PUBLISHING.md](PUBLISHING.md)** for publishing to npm and listing on the MCP registry, Smithery, Glama, Cursor, and PulseMCP.

## License

MIT
