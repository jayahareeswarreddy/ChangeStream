# ChangeStream project prompt

Build a portfolio-ready, local-first PostgreSQL CDC demonstrator. PostgreSQL is the source of truth. Debezium must capture committed row changes from PostgreSQL logical replication and publish them to Kafka. A Python consumer must project those events into a separate read-model database, recording every processed event and exposing duplicate-safe, at-least-once processing. A FastAPI service and small dashboard must show source records, recent events, consumer health, lag, and quarantined poison events.

Keep the first version understandable and runnable with Docker Compose on one machine. Use explicit service names, health/readiness handling, persistent volumes, an idempotent connector registration command, seeded sample orders, and clear shutdown/reset instructions. Keep credentials clearly marked as local-demo defaults. Do not imply exactly-once delivery, production security, or cloud hosting. Document the event envelope, delete/tombstone behavior, crash/replay semantics, and known limits. Build only what can be explained and verified; update the README when behavior or configuration changes.

## First milestone acceptance criteria

- One documented command starts PostgreSQL, Kafka, Kafka Connect/Debezium, the consumer, and API/dashboard.
- A documented command registers a PostgreSQL connector configured for `pgoutput` and only the sample `orders` table.
- Insert, update, and delete operations in the source table become Debezium events and update the separate read model.
- Kafka offsets are committed only after the read-model transaction succeeds; replays are safe using topic/partition/offset uniqueness.
- The API reports heartbeat, consumer lag, processed/failed counts, recent events, current orders, and dead-letter records.
- The README gives a copy-pasteable walkthrough and labels the stack as a local demo.
