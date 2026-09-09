# mcp-kafka

[![CI](https://github.com/dockndevai/mcp-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/dockndevai/mcp-kafka/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![npm](https://img.shields.io/npm/v/@dockndevai/mcp-kafka)](https://www.npmjs.com/package/@dockndevai/mcp-kafka)

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
| Interactive confirmation | *(automatic)* | — | Destructive & high-impact actions prompt the human to approve via MCP elicitation before running; clients without elicitation fall back to the `*_ALLOW_*` gate. |

## Tools

**Read** (`read-only`+): `cluster_info`, `list_topics`, `describe_topic`, `topic_offsets`, `list_consumer_groups`, `describe_consumer_group` (with lag)

**Write** (`read-write`+): `create_topic`, `create_partitions`, `alter_topic_config`, `reset_consumer_group_offsets`

**Admin** (`admin`): `delete_topic`, `delete_consumer_group` (both need `KAFKA_ALLOW_DELETE`)

## Quickstart — add to your agent

Published on npm as [`@dockndevai/mcp-kafka`](https://www.npmjs.com/package/@dockndevai/mcp-kafka). No clone or build needed — your MCP client runs it on demand with `npx`. **Start in `read-only` mode**; see [`.env.example`](.env.example) for every variable and [docs/CLIENTS.md](docs/CLIENTS.md) for the full per-client guide.

**Claude Code** (CLI)

```bash
claude mcp add kafka -e KAFKA_BROKERS="localhost:9092" -e KAFKA_MODE="read-only" -- npx -y @dockndevai/mcp-kafka
```

**Claude Desktop · Cursor · Windsurf** — same block in `claude_desktop_config.json`, `.cursor/mcp.json`, or `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "kafka": {
      "command": "npx",
      "args": [
        "-y",
        "@dockndevai/mcp-kafka"
      ],
      "env": {
        "KAFKA_BROKERS": "localhost:9092",
        "KAFKA_MODE": "read-only"
      }
    }
  }
}
```

**OpenAI Codex CLI** — in `~/.codex/config.toml`:

```toml
[mcp_servers.kafka]
command = "npx"
args = ["-y", "@dockndevai/mcp-kafka"]
env = { KAFKA_BROKERS = "localhost:9092", KAFKA_MODE = "read-only" }
```

**VS Code (GitHub Copilot, Agent mode)** — in `.vscode/mcp.json`:

```json
{
  "servers": {
    "kafka": {
      "type": "stdio",
      "command": "npx",
      "args": [
        "-y",
        "@dockndevai/mcp-kafka"
      ],
      "env": {
        "KAFKA_BROKERS": "localhost:9092",
        "KAFKA_MODE": "read-only"
      }
    }
  }
}
```

## Example prompts

- *"Which consumer groups have the most lag right now?"*
- *"Describe the `orders` topic and show its offsets."*
- *"Create a topic `events` with 6 partitions and 7-day retention."* (needs `read-write`)

## Run from source (development)

Prefer the published package above. To run from a clone:

```bash
npm install
npm run build
node dist/index.js   # with the environment variables set
```

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
