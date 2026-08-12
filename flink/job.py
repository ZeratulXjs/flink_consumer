# PyFlink DataStream job: session_submitted (Kafka) -> Postgres.
#
# Pipeline shape (see CLAUDE.md "Architecture"):
#   KafkaSource(session_submitted)
#     -> ParseAndRoute (ProcessFunction)
#          - valid payloads  -> main output, as a Row
#          - parse failures  -> side output -> KafkaSink(session_submitted.dlq)
#     -> key_by(group_id, volunteer_id, week_number)
#     -> VersionAssigner (KeyedProcessFunction, keyed ValueState)
#          - assigns version = max(seen so far for this key) + 1
#     -> JdbcSink -> session_events (append, one row per version)
#     -> JdbcSink -> session_state  (upsert, version-guarded ON CONFLICT)
#
# Parsing (flink/parser.py) and version assignment (flink/versioning.py) are
# plain Python, independently unit testable without a Flink runtime -- this
# file only wires them into Flink's source/process/sink graph.
import json
import logging
import os

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

from parser import ParseError, build_dlq_envelope, parse_session_submission
from versioning import next_version

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("session_submitted_job")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "session_submitted")
KAFKA_DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "session_submitted.dlq")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "flink-consumer-group")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "session_tracker")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "session_tracker")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "changeme")

DLQ_OUTPUT_TAG = OutputTag("dlq", Types.STRING())

PARSED_ROW_TYPE = Types.ROW_NAMED(
    ["group_id", "volunteer_id", "week_number", "submitted_at", "session_date", "attendance", "session_notes"],
    [Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()],
)

VERSIONED_ROW_TYPE = Types.ROW_NAMED(
    ["group_id", "volunteer_id", "week_number", "version", "submitted_at", "session_date", "attendance", "session_notes"],
    [Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()],
)

SESSION_EVENTS_INSERT_SQL = """
INSERT INTO session_events (group_id, volunteer_id, week_number, version, submitted_at, session_date, attendance, session_notes)
VALUES (?, ?, ?, ?, ?::TIMESTAMPTZ, ?::DATE, ?::JSONB, ?)
"""

SESSION_STATE_UPSERT_SQL = """
INSERT INTO session_state (group_id, volunteer_id, week_number, version, submitted_at, session_date, attendance, session_notes)
VALUES (?, ?, ?, ?, ?::TIMESTAMPTZ, ?::DATE, ?::JSONB, ?)
ON CONFLICT (group_id, week_number) DO UPDATE SET
    volunteer_id = EXCLUDED.volunteer_id,
    version = EXCLUDED.version,
    submitted_at = EXCLUDED.submitted_at,
    session_date = EXCLUDED.session_date,
    attendance = EXCLUDED.attendance,
    session_notes = EXCLUDED.session_notes
WHERE EXCLUDED.version > session_state.version
    OR (EXCLUDED.version = session_state.version AND EXCLUDED.submitted_at > session_state.submitted_at)
"""


class ParseAndRoute(ProcessFunction):
    """Parses each raw Kafka record; valid ones go to the main output as a
    Row, failures go to the DLQ side output as a JSON-encoded envelope
    string. Never drops a message -- every input reaches exactly one of the
    two outputs (per CLAUDE.md, DLQ instead of a silent filter/drop)."""

    def process_element(self, value, ctx):
        try:
            parsed = parse_session_submission(value)
        except ParseError as exc:
            envelope = build_dlq_envelope(value, KAFKA_TOPIC, exc.step, exc.message)
            ctx.output(DLQ_OUTPUT_TAG, json.dumps(envelope))
            logger.warning("routed to dlq step=%s error=%s", exc.step, exc.message)
            return
        except Exception as exc:  # noqa: BLE001 - unexpected failures must still reach the DLQ, never drop
            envelope = build_dlq_envelope(value, KAFKA_TOPIC, "unexpected", str(exc))
            ctx.output(DLQ_OUTPUT_TAG, json.dumps(envelope))
            logger.error("routed to dlq step=unexpected error=%s", exc)
            return
        logger.info(
            "parsed submission key=%s:%s:%s",
            parsed.group_id, parsed.volunteer_id, parsed.week_number,
        )
        yield Row(
            group_id=parsed.group_id,
            volunteer_id=parsed.volunteer_id,
            week_number=parsed.week_number,
            submitted_at=parsed.submitted_at,
            session_date=parsed.session_date,
            attendance=json.dumps(parsed.attendance),
            session_notes=parsed.session_notes,
        )


class VersionAssigner(KeyedProcessFunction):
    """Keyed on (group_id, volunteer_id, week_number). Tracks the max
    version seen so far per key in Flink keyed state and assigns the next
    one -- pure in-stream, no DB round-trip (per CLAUDE.md versioning
    section)."""

    def open(self, runtime_context):
        self.max_version_state = runtime_context.get_state(
            ValueStateDescriptor("max_version_seen", Types.INT())
        )

    def process_element(self, value, ctx):
        current_max = self.max_version_state.value()
        version = next_version(current_max)
        self.max_version_state.update(version)
        logger.info(
            "assigned version key=%s:%s:%s version=%s",
            value.group_id, value.volunteer_id, value.week_number, version,
        )
        yield Row(
            group_id=value.group_id,
            volunteer_id=value.volunteer_id,
            week_number=value.week_number,
            version=version,
            submitted_at=value.submitted_at,
            session_date=value.session_date,
            attendance=value.attendance,
            session_notes=value.session_notes,
        )


def build_jdbc_connection_options():
    return JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_url(f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
        .with_driver_name("org.postgresql.Driver") \
        .with_user_name(POSTGRES_USER) \
        .with_password(POSTGRES_PASSWORD) \
        .build()


def build_jdbc_execution_options():
    # Demo-scale batching: smaller/faster than the driver_trackables_flink
    # reference (batch size 200 / 1000ms) so writes flush quickly and stay
    # visible while recording a demo, per CLAUDE.md "Demo & presentation".
    return JdbcExecutionOptions.builder() \
        .with_batch_size(100) \
        .with_batch_interval_ms(1000) \
        .with_max_retries(5) \
        .build()


def build_dlq_sink():
    return KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id(KAFKA_CONSUMER_GROUP) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    raw_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "session_submitted-source")

    parsed_stream = raw_stream.process(ParseAndRoute(), output_type=PARSED_ROW_TYPE)

    parsed_stream.get_side_output(DLQ_OUTPUT_TAG).sink_to(build_dlq_sink()).name("dlq-sink")

    versioned_stream = parsed_stream \
        .key_by(lambda row: (row.group_id, row.volunteer_id, row.week_number)) \
        .process(VersionAssigner(), output_type=VERSIONED_ROW_TYPE)

    versioned_stream.add_sink(
        JdbcSink.sink(
            SESSION_EVENTS_INSERT_SQL,
            VERSIONED_ROW_TYPE,
            build_jdbc_connection_options(),
            build_jdbc_execution_options(),
        )
    ).name("session-events-sink")

    versioned_stream.add_sink(
        JdbcSink.sink(
            SESSION_STATE_UPSERT_SQL,
            VERSIONED_ROW_TYPE,
            build_jdbc_connection_options(),
            build_jdbc_execution_options(),
        )
    ).name("session-state-sink")

    env.execute("session_submitted -> postgres")


if __name__ == "__main__":
    main()
