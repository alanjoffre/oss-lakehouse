"""Log estruturado (JSON por linha) com a biblioteca padrão.

Por que JSON: o agregador (Azure Monitor/Log Analytics, Datadog, ELK) indexa cada campo e
permite filtrar `run_id = X and status = 'error'` sem regex em texto livre.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

# Atributos que todo LogRecord tem; o resto veio de `extra=` e vira campo do JSON.
_STANDARD_ATTRS = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _STANDARD_ATTRS:
                doc[key] = value
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, ensure_ascii=False, default=str)


def json_logger(name: str, stream: object | None = None, level: int = logging.INFO) -> logging.Logger:
    """Logger com um único handler JSON (idempotente: chamar 2x não duplica o handler)."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    for h in list(logger.handlers):
        logger.removeHandler(h)
    handler = logging.StreamHandler(stream)  # type: ignore[arg-type]
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger
