# Pure parsing/validation logic for a raw session_submitted payload, kept
# free of any PyFlink imports so it can be unit tested without a Flink
# runtime (see CLAUDE.md "Testing & coverage"). flink/job.py wires this into
# the Flink ProcessFunction that does the actual DLQ routing.
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

REQUIRED_FIELDS = (
    "group_id",
    "volunteer_id",
    "week_number",
    "submitted_at",
    "session_date",
    "attendance",
)

VALID_ATTENDANCE_STATUSES = ("present", "absent")


class ParseError(Exception):
    def __init__(self, step: str, message: str):
        self.step = step
        self.message = message
        super().__init__(f"[{step}] {message}")


@dataclass
class ParsedSubmission:
    group_id: str
    volunteer_id: str
    week_number: int
    submitted_at: str
    session_date: str
    attendance: list
    session_notes: Optional[str]


def parse_session_submission(raw: str) -> ParsedSubmission:
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ParseError("parse_json", str(exc)) from exc

    if not isinstance(data, dict):
        raise ParseError("validate_shape", "payload is not a JSON object")

    missing = [field for field in REQUIRED_FIELDS if data.get(field) in (None, "")]
    if missing:
        raise ParseError("validate_required_fields", f"missing required field(s): {', '.join(missing)}")

    attendance = data["attendance"]
    if not isinstance(attendance, list) or not attendance:
        raise ParseError("validate_attendance", "attendance must be a non-empty list")
    for entry in attendance:
        if not isinstance(entry, dict) or not entry.get("participant_id"):
            raise ParseError("validate_attendance", f"attendance entry missing participant_id: {entry!r}")
        if entry.get("status") not in VALID_ATTENDANCE_STATUSES:
            raise ParseError("validate_attendance", f"attendance status must be present/absent, got: {entry.get('status')!r}")

    try:
        week_number = int(data["week_number"])
    except (TypeError, ValueError) as exc:
        raise ParseError("validate_week_number", f"week_number must be an integer: {exc}") from exc
    if not (1 <= week_number <= 6):
        raise ParseError("validate_week_number", f"week_number out of range 1-6: {week_number}")

    return ParsedSubmission(
        group_id=str(data["group_id"]),
        volunteer_id=str(data["volunteer_id"]),
        week_number=week_number,
        submitted_at=str(data["submitted_at"]),
        session_date=str(data["session_date"]),
        attendance=attendance,
        session_notes=data.get("session_notes"),
    )


def build_dlq_envelope(raw: str, source_topic: str, failed_step: str, error_message: str) -> dict:
    return {
        "raw_payload": raw,
        "failure": {
            "source_topic": source_topic,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error_message": error_message,
            "failed_step": failed_step,
        },
    }
