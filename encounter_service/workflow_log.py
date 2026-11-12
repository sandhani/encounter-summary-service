"""Content-free JSON workflow events, emitted after durable state transitions."""
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger('encounter_service.workflow')


def emit(event, **fields):
    logger.info(json.dumps(dict(timestamp=datetime.now(timezone.utc).isoformat(),
                                event=event, worker_pid=os.getpid(), **fields),
                           separators=(',', ':')))
