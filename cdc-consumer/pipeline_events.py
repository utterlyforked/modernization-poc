"""
Best-effort pipeline event log (table `pipeline_event`) for the web UI's activity pane.

Events are written on their own autocommit connection so they can never join, commit or roll back the
consumer's staging transaction. Every failure is swallowed: an event that cannot be written is lost, the
pipeline carries on.
"""

import json
import time

import psycopg2

MAX_AGE = '24 hours'    # events older than this are pruned
MAX_ROWS = 5000         # ...and so is everything beyond the newest MAX_ROWS
PRUNE_INTERVAL = 60     # seconds between prunes
RETRY_INTERVAL = 5      # seconds to wait before reconnecting after a failure

CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS pipeline_event (
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        source VARCHAR(50) NOT NULL,
        stage VARCHAR(50) NOT NULL,
        severity VARCHAR(10) NOT NULL DEFAULT 'info',
        tenant_id VARCHAR(50),
        legacy_id INTEGER,
        message TEXT NOT NULL,
        detail JSONB
    )
"""

PRUNE_SQL = """
    DELETE FROM pipeline_event
    WHERE ts < now() - %s::interval
       OR id <= (SELECT max(id) FROM pipeline_event) - %s
"""


class EventWriter:
    def __init__(self, db_config, source):
        self.db_config = db_config
        self.source = source
        self.conn = None
        self.last_prune = 0
        self.next_attempt = 0

    def _connect(self):
        conn = psycopg2.connect(connect_timeout=3, **self.db_config)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(CREATE_SQL)
        cursor.close()
        return conn

    def emit(self, stage, message, severity='info', tenant_id=None, legacy_id=None, detail=None):
        """Record one event. Never raises."""
        try:
            if self.conn is None:
                if time.time() < self.next_attempt:
                    return
                self.conn = self._connect()
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO pipeline_event (source, stage, severity, tenant_id, legacy_id, message, detail) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (self.source, stage, severity, tenant_id, legacy_id, message,
                 json.dumps(detail, default=str) if detail is not None else None))
            if time.time() - self.last_prune >= PRUNE_INTERVAL:
                self.last_prune = time.time()
                cursor.execute(PRUNE_SQL, (MAX_AGE, MAX_ROWS))
            cursor.close()
        except Exception as e:
            print(f"pipeline event not written: {e}", flush=True)
            self.next_attempt = time.time() + RETRY_INTERVAL
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
