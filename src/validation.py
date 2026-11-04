"""Input validation for device events (pure functions, easy to unit test)."""
from datetime import datetime, timezone

MAX_DEVICE_ID_LENGTH = 64


class ValidationError(ValueError):
    """Raised when an incoming event does not meet the contract."""


def validate_event(payload: dict) -> dict:
    """Return a normalized event or raise ValidationError."""
    if not isinstance(payload, dict):
        raise ValidationError("payload must be a JSON object")

    device_id = payload.get("device_id")
    if not isinstance(device_id, str) or not device_id.strip():
        raise ValidationError("device_id is required")
    if len(device_id) > MAX_DEVICE_ID_LENGTH:
        raise ValidationError("device_id is too long")

    reading = payload.get("reading")
    if isinstance(reading, bool) or not isinstance(reading, (int, float)):
        raise ValidationError("reading must be a number")

    # Default the timestamp server-side so every stored event has one.
    event_ts = payload.get("event_ts") or datetime.now(timezone.utc).isoformat()
    try:
        datetime.fromisoformat(event_ts)
    except (TypeError, ValueError):
        raise ValidationError("event_ts must be an ISO-8601 timestamp")

    return {"device_id": device_id.strip(), "reading": float(reading), "event_ts": event_ts}
