# Pure versioning logic, kept free of any PyFlink imports so it can be unit
# tested without a Flink runtime (see CLAUDE.md "Testing & coverage").
#
# next_version() is what flink/job.py's KeyedProcessFunction calls against
# its ValueState (the "max version seen for this natural key") on every
# incoming event -- the state lookup/update is Flink plumbing, this function
# is the actual decision logic.
#
# should_overwrite() documents the same guard that is enforced for real in
# the session_state JDBC sink's ON CONFLICT ... WHERE clause (see
# flink/job.py SESSION_STATE_UPSERT_SQL). It's kept here too, as a plain
# function, so the conflict-resolution semantics are independently unit
# testable rather than only checkable by reading SQL.
from datetime import datetime
from typing import Optional, Union

Timestamp = Union[str, datetime]


def next_version(current_max: Optional[int]) -> int:
    return 1 if current_max is None else current_max + 1


def _parse_timestamp(value: Timestamp) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def should_overwrite(
    incoming_version: int,
    incoming_submitted_at: Timestamp,
    stored_version: int,
    stored_submitted_at: Timestamp,
) -> bool:
    if incoming_version != stored_version:
        return incoming_version > stored_version
    return _parse_timestamp(incoming_submitted_at) > _parse_timestamp(stored_submitted_at)
