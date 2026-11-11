"""Small, non-invasive background job helpers for NonoDB.

This package does not extend the daemon. Jobs are ordinary documents in a
regular collection, and workers are ordinary clients using the HTTP/UDS API.
"""

from .events import (
    EventConsumer,
    EventProducer,
    EventRunner,
    event_timestamp_ms,
    new_event_id,
)
from .jobs import (
    DEFAULT_MAX_ATTEMPTS,
    Job,
    JobQueue,
    JobRunner,
    backoff_seconds,
    now_ms,
)

__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "EventConsumer",
    "EventProducer",
    "EventRunner",
    "Job",
    "JobQueue",
    "JobRunner",
    "backoff_seconds",
    "event_timestamp_ms",
    "new_event_id",
    "now_ms",
]
