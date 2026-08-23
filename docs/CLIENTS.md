# Installing `mcp-kafka` in your MCP client

`mcp-kafka` is a **stdio** MCP server. Any MCP-compatible agent can run it. Two ways to launch it:

- **From source (works today):** `node /ABSOLUTE/PATH/TO/mcp-kafka/dist/index.js` after `npm install && npm run build`.
- **From npm (after it is published):** `npx -y mcp-kafka` — replace the `command`/`args` below with `"command": "npx", "args": ["-y", "mcp-kafka"]`.

> Replace `/ABSOLUTE/PATH/TO/mcp-kafka` with the real absolute path on your machine, and set the environment variables for your cluster/instance. **Start in `read-only` mode** and raise it deliberately. See [`.env.example`](../.env.example) for every supported variable.

## Prerequisites

```bash
cd mcp-kafka
npm install
npm run build
```

## Claude Code (CLI)

```bash
claude mcp add kafka \
  -e KAFKA_BROKERS="localhost:9092" \
  -e KAFKA_MODE="read-only" \
  -- node /ABSOLUTE/PATH/TO/mcp-kafka/dist/index.js
```

Add `-s user` to install it for all your projects, or `-s project` to write it into a shared `.mcp.json`. List with `claude mcp list`, remove with `claude mcp remove kafka`.

## Claude Desktop

Edit `claude_desktop_config.json` (macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`) and merge:

```json
{
  "mcpServers": {
    "kafka": {
      "command": "node",
      "args": [
        "/ABSOLUTE/PATH/TO/mcp-kafka/dist/index.js"
      ],
      "env": {
        "KAFKA_BROKERS": "localhost:9092",
        "KAFKA_MODE": "read-only"
      }
    }
  }
}
```

Restart Claude Desktop. The server appears under the tools (🔨) menu.

## Cursor

Create `.cursor/mcp.json` in your project (or `~/.cursor/mcp.json` for all projects):

```json
{
  "mcpServers": {
    "kafka": {
      "command": "node",
      "args": [
        "/ABSOLUTE/PATH/TO/mcp-kafka/dist/index.js"
      ],
      "env": {
        "KAFKA_BROKERS": "localhost:9092",
        "KAFKA_MODE": "read-only"
      }
    }
  }
}
```

Then enable it in **Cursor Settings → MCP**.

## OpenAI Codex CLI

Edit `~/.codex/config.toml` and add:

```toml
[mcp_servers.kafka]
command = "node"
args = ["/ABSOLUTE/PATH/TO/mcp-kafka/dist/index.js"]
env = { KAFKA_BROKERS = "localhost:9092", KAFKA_MODE = "read-only" }
```

Codex reads MCP servers from `config.toml` on startup.

## Windsurf

Edit `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "kafka": {
      "command": "node",
      "args": [
        "/ABSOLUTE/PATH/TO/mcp-kafka/dist/index.js"
      ],
      "env": {
        "KAFKA_BROKERS": "localhost:9092",
        "KAFKA_MODE": "read-only"
      }
    }
  }
}
```

Then **Refresh** in the Windsurf MCP settings panel.

## VS Code (GitHub Copilot / Agent mode)

Create `.vscode/mcp.json` (note the top-level key is `servers`):

```json
{
  "servers": {
    "kafka": {
      "type": "stdio",
      "command": "node",
      "args": [
        "/ABSOLUTE/PATH/TO/mcp-kafka/dist/index.js"
      ],
      "env": {
        "KAFKA_BROKERS": "localhost:9092",
        "KAFKA_MODE": "read-only"
      }
    }
  }
}
```

Open the Copilot Chat **Agent** view and confirm the server is listed.

## Any other MCP client

Point it at the command `node /ABSOLUTE/PATH/TO/mcp-kafka/dist/index.js` (transport: **stdio**) with the same environment variables.

## Verify

On startup the server logs a line to **stderr** like:

```
[kafka-mcp] Starting in 'read-only' mode. N tools enabled: …
```

If you see `Configuration error: …` instead, fix the reported variable. Ask your agent to *"list the Kafka tools"* to confirm the connection.
