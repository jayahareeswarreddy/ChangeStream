# ChangeStream

ChangeStream is a local-first CDC pipeline demo: PostgreSQL changes are read from the WAL by Debezium, published to Kafka, then applied to a separate PostgreSQL read model by a Python consumer. A FastAPI service and responsive, system-adaptive dashboard expose the pipeline stages, current projection, recent changes, consumer health, Kafka lag, and quarantined records. The dashboard follows the device light/dark appearance and uses CSS surfaces and gradients rather than a generated background image.

This repository is an educational single-node demo, not a production deployment. Its sample credentials, exposed ports, single Kafka broker, and unencrypted connections are intended only for a local machine.

## Architecture

```text
PostgreSQL source (source_db)
    │ logical replication · pgoutput
    ▼
Debezium PostgreSQL connector (Kafka Connect)
    │ topic: cdc.public.orders
    ▼
Kafka
    │ manual offset commit after DB transaction
    ▼
Python consumer ──► PostgreSQL read model (read_model)
                         │
                         └── FastAPI ──► dashboard
                              ├── current orders
                              ├── processed event log
                              ├── live consumer lag + heartbeat
                              └── dead-letter records
```

The PostgreSQL connector uses the built-in `pgoutput` logical decoding plugin. The publication is created by the source database init script; the connector is configured not to create or alter it. Kafka Connect runs with single-broker internal-topic replication factors for this local Compose setup.

## Requirements

- Docker Desktop with Docker Compose v2
- Python 3.10+ on the host for the standard-library connector registration script

## Start the demo

From this directory, copy the example environment file and start the services:

```powershell
Copy-Item .env.example .env
docker compose up --build -d
```

Register or update the Debezium connector. This script waits for Kafka Connect to become ready and can be run again safely:

```powershell
python scripts/register_connector.py
```

Confirm the connector is running:

```powershell
curl.exe http://localhost:8083/connectors/changestream-orders/status
```

Open the dashboard at <http://localhost:8010>. Select **How it works** for a dedicated guide to the architecture, startup, database changes, Kafka inspection, event operations, and cleanup. The API is available at <http://localhost:8010/docs>. The pipeline diagram reads live health from PostgreSQL, Kafka, the Debezium connector, the consumer, and the read model. Select **Inspect connector** under Debezium to see Kafka Connect's connector and task state.

The first connector start takes a snapshot of the seeded `public.orders` table. Snapshot records use Debezium's read operation (`r`); later database inserts, updates, and deletes use `c`, `u`, and `d`.

To inspect the running services from PowerShell:

```powershell
Invoke-RestMethod http://localhost:8010/api/services | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:8083/connectors/changestream-orders/status | ConvertTo-Json -Depth 5
docker compose exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic cdc.public.orders --from-beginning --max-messages 3
```

The first command reports each component's live health. The second is Debezium's connector/task status from Kafka Connect. The third reads actual serialized change records from Kafka. Kafka is a broker, so this local stack does not include a separate Kafka web console.

## Make a source change

Insert a row:

```powershell
docker compose exec postgres psql -U postgres -d source_db -c "INSERT INTO orders (customer_name, status, total_cents) VALUES ('Jaya', 'created', 8900);"
```

Update one of the seeded rows:

```powershell
docker compose exec postgres psql -U postgres -d source_db -c "UPDATE orders SET status = 'paid', updated_at = NOW() WHERE id = 1;"
```

Delete a row:

```powershell
docker compose exec postgres psql -U postgres -d source_db -c "DELETE FROM orders WHERE id = 2;"
```

Watch the current orders and change log at <http://localhost:8010>. The API docs are at <http://localhost:8010/docs>. You can also inspect the raw Kafka topic from the broker container:

```powershell
docker compose exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic cdc.public.orders --from-beginning
```

## See consumer lag

Stop the consumer, make several source changes, and watch the lag increase while the consumer is offline. The API reads committed consumer offsets and Kafka's current end offsets directly from the broker, so this value continues updating while the consumer is stopped.

```powershell
docker compose stop consumer
docker compose exec postgres psql -U postgres -d source_db -c "INSERT INTO orders (customer_name, status, total_cents) VALUES ('Lag Demo', 'created', 1250);"
```

Start the consumer again and watch the lag return toward zero as the read model catches up:

```powershell
docker compose start consumer
```

## Processing and replay behavior

- The consumer disables Kafka auto-commit. It applies each event and records the topic, partition, and offset in the same PostgreSQL transaction, then commits the Kafka offset.
- If the consumer stops after the database commit but before the Kafka offset commit, Kafka can deliver that record again. The unique `(topic, partition, offset)` key makes projection replay idempotent.
- Debezium emits a delete event followed by a null tombstone. The consumer applies and records the delete event, then commits the tombstone without treating it as another row change.
- Invalid JSON or unsupported event shapes are written to `dead_letters` and then committed, preventing a poison message from blocking the partition. A database outage is not quarantined: the offset remains uncommitted and the event is retried.
- Delivery is **at least once**. This demo does not claim distributed exactly-once processing.

## API endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /guide` | Architecture and hands-on usage guide |
| `GET /api/services` | Live source/read-model PostgreSQL, Kafka broker/topic, Debezium connector/task, and consumer status |
| `GET /api/health` | Consumer heartbeat, online state, broker-derived lag, applied/failed counts, latest event and error |
| `GET /api/orders` | Current read-model orders |
| `GET /api/events?limit=50` | Recent successfully applied Debezium events |
| `GET /api/dead-letters?limit=25` | Quarantined malformed or unsupported records |
| `GET /docs` | Interactive FastAPI API documentation |

## Reset local demo data

This deletes the local PostgreSQL and Kafka volumes and returns the project to its seeded state:

```powershell
docker compose down -v
docker compose up --build -d
python scripts/register_connector.py
```

## Current scope and limitations

- One PostgreSQL table, one Kafka broker, one topic partition, and one consumer group.
- Compose credentials and local ports are not production-safe; do not put real customer data into this demo.
- The consumer projects the sample order fields. Schema changes require updating the read-model projection.
- Consumer lag is computed from Kafka's committed group offsets and topic end offsets; a brand-new topic/group may report unavailable until Kafka metadata and the first group offsets exist.
- A free public deployment is not part of this first milestone. Kafka, PostgreSQL, and Connect need persistent state and coordinated networking; establish the local pipeline before choosing hosting.

## References

- [Debezium PostgreSQL connector documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Debezium's PostgreSQL Docker Compose tutorial](https://github.com/debezium/debezium-examples/blob/main/tutorial/docker-compose-postgres.yaml)
- [Confluent Python consumer and AdminClient documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html)

## Project objective

See [PROJECT_PROMPT.md](PROJECT_PROMPT.md) for the constraints and acceptance criteria guiding this implementation.
