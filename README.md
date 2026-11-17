# flink_consumer

Local, Docker-based Flink consumer for messages brokered by RabbitMQ and stored in PostgreSQL.

The whole stack runs on one machine with Docker Compose, so it can be built, tested and changed without any cloud services.

## Architecture

```
RabbitMQ -> bridge -> Kafka -> PyFlink job -> PostgreSQL
```

A small bridge service forwards messages from RabbitMQ into Kafka. The PyFlink job reads from Kafka, parses each message, and writes the result to PostgreSQL.

## Repository layout

| Path | Purpose |
|---|---|
| `bridge/` | RabbitMQ to Kafka bridge (`rmq_kafka_bridge.py`) with its own Dockerfile and requirements |
| `flink/` | PyFlink job (`job.py`), message parsing (`parser.py`) and `versioning.py` [one line on what it does] |
| `db/` | `schema.sql` for the target tables and `seed_roster.sql` with synthetic sample data |
| `env/` | `.env.example`, the configuration template |
| `docker-compose.yml` | Starts all services locally |
| `tests/` | Placeholders for unit and integration tests |

## Quick start

Requires Docker and Docker Compose.

```bash
cp env/.env.example env/.env    # then edit the values
docker compose up --build
```

## Configuration

Settings are read from the environment. `env/.env.example` lists them all.

| Variable | Purpose |
|---|---|
| [NAME] | [what it controls] |

Never commit a real `.env` file.

## Data

Input: [short, synthetic example of one message].

Output: [table name, key columns, and how duplicate messages are handled].

## Status

Working local build. Tests are not written yet; `tests/unit` and `tests/integration` are placeholders.

## Licence

MIT License. See `LICENSE`.