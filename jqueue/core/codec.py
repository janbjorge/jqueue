"""
Codec — serialize and deserialize QueueState to/from bytes using Pydantic v2.

Pydantic v2 handles the full wire format automatically:
  - bytes fields are encoded as base64 strings in JSON mode
  - datetime fields are serialized as ISO-8601 strings with UTC offset
  - Enum values are serialized as their string values
  - Nested models (Job inside QueueState) are recursively serialized

Wire format (produced by model_dump_json):
------------------------------------------
{
  "version": 3,
  "jobs": [
    {
      "id": "550e8400-...",
      "entrypoint": "send_email",
      "payload": "SGVsbG8gV29ybGQ=",   <-- base64-encoded bytes
      "status": "queued",
      "priority": 0,
      "created_at": "2024-01-01T00:00:00+00:00",
      "heartbeat_at": null
    }
  ]
}
"""
from __future__ import annotations

from jqueue.domain.models import QueueState


def encode(state: QueueState) -> bytes:
    """Serialize QueueState to UTF-8 JSON bytes."""
    return state.model_dump_json(indent=2).encode("utf-8")


def decode(data: bytes) -> QueueState:
    """Deserialize UTF-8 JSON bytes to QueueState. Empty bytes → empty state."""
    if not data:
        return QueueState()
    return QueueState.model_validate_json(data)
