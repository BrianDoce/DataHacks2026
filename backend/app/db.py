"""
Databricks SQL connection management.

Holds a single module-level connection and reopens it transparently on any
transport failure. Concurrency is low (demo / hackathon tier) so a single
shared connection with a threading lock is sufficient — one cursor is opened
and closed per request inside get_cursor().

Environment variables (required, never hardcoded):
  DATABRICKS_HOST        workspace URL, e.g. https://adb-123.azuredatabricks.net
  DATABRICKS_HTTP_PATH   SQL warehouse HTTP path, e.g. /sql/1.0/warehouses/abc
  DATABRICKS_TOKEN       personal access token or service-principal OAuth token
"""

import logging
import os
import threading
from contextlib import contextmanager
from typing import Generator, Optional

import databricks.sql
import databricks.sql.client as _dbc

logger = logging.getLogger(__name__)

# ── Module-level singleton ────────────────────────────────────────────────────
_lock: threading.Lock = threading.Lock()
_conn: Optional[_dbc.Connection] = None


def _hostname() -> str:
    host = os.environ["DATABRICKS_HOST"]
    # Strip protocol prefix — the connector expects bare hostname only.
    return host.removeprefix("https://").removeprefix("http://").rstrip("/")


def _open() -> _dbc.Connection:
    return databricks.sql.connect(
        server_hostname=_hostname(),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def init() -> None:
    """Open the connection eagerly (called from app lifespan)."""
    global _conn
    with _lock:
        if _conn is None:
            logger.info("Opening Databricks SQL connection …")
            _conn = _open()
            logger.info("Databricks SQL connection ready.")


def close() -> None:
    """Close the connection gracefully (called from app lifespan teardown)."""
    global _conn
    with _lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
            logger.info("Databricks SQL connection closed.")


@contextmanager
def get_cursor() -> Generator[_dbc.Cursor, None, None]:
    """
    Yield an open cursor, closing it when the block exits.

    If the underlying connection is dead the function reopens it once before
    raising so that the next request succeeds.
    """
    global _conn

    with _lock:
        if _conn is None:
            _conn = _open()
        conn = _conn

    cursor: Optional[_dbc.Cursor] = None
    try:
        cursor = conn.cursor()
        yield cursor
    except Exception as exc:
        # Connection-level errors → drop and recreate on the next call.
        logger.warning("Databricks cursor error (%s); resetting connection.", exc)
        with _lock:
            if _conn is conn:
                try:
                    conn.close()
                except Exception:
                    pass
                _conn = None
        raise
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
