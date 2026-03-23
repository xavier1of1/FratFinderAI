from __future__ import annotations

import json
import logging
from datetime import datetime, timezone


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format="%(message)s",
    )


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields: object) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": logging.getLevelName(level),
        "logger": logger.name,
        "event": event,
        **fields,
    }
    logger.log(level, json.dumps(payload, default=str, sort_keys=True))
