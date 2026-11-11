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
- **Require a human for destructive ops.** Set `KAFKA_REQUIRE_HUMAN_APPROVAL=true` so `delete_topic`,
  `delete_consumer_group` and offset resets are refused when the client cannot show an approval prompt.
  Each approval is bound to the exact tool and arguments (fingerprint) and to the target's version
  (partition layout / group membership / committed offsets); if the target changed while the human was
  deciding, the action is refused. `delete_topic` also asks the human to type the topic name. Approval
  receipts are written to the audit log (`"audit":"approval"`, with `approvedBy` = `human` or `flag-only`).
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
