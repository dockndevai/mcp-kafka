# Security

`mcp-kafka` exposes Kafka cluster administration to an AI agent. Treat it like
any other privileged automation and grant it the least access it needs.

## Principles

- **Start read-only.** Leave `KAFKA_MODE=read-only` until you specifically need
  the agent to make changes. Tools above the current mode are never registered.
- **Scope with ACLs, not just flags.** The flags are defence in depth; the
  primary control is Kafka ACLs on the SASL principal the server authenticates as.
- **Protect internal and critical topics.** Internal topics (`__consumer_offsets`,
  `__transaction_state`, …) are protected by default; add business-critical topics
  to `KAFKA_PROTECTED_TOPICS`. Protected topics can be read but never mutated.
- **Constrain scope.** Use `KAFKA_TOPIC_ALLOWLIST` to limit which topics the agent
  can touch at all.
- **Gate deletion explicitly.** `delete_topic` and `delete_consumer_group` require
  both `admin` mode and `KAFKA_ALLOW_DELETE=true`.
- **Preview with dry-run.** `KAFKA_DRY_RUN=true` validates and logs write intent
  without contacting the cluster.
- **Keep the audit log on.** `KAFKA_AUDIT_LOG=true` (default) writes a JSON line
  per guarded operation to stderr.

## Handling of credentials

- SASL credentials are read from environment variables and never logged or
  returned in tool results.

## Reporting a vulnerability

Please open a private security advisory on the GitHub repository rather than a
public issue.
