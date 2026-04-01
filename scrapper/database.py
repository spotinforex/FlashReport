import logging
import os
import sys
import time
from contextlib import contextmanager
from typing import Optional, List, Dict, Any

import psycopg2
import psycopg2.pool
from psycopg2.extras import execute_batch, RealDictCursor
from dotenv import load_dotenv

logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

load_dotenv()

# Errors that indicate a dead/stale connection — safe to retry
_STALE_CONN_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)


class Database:
    _pool = None
    _pool_lock = __import__('threading').Lock()

    ALLOWED_TABLES = {
        'sources', 'parsed_articles', 'signals',
        'events', 'event_articles', 'analysis'
    }

    def __init__(self):
        if Database._pool is None:
            self._initialize_pool()
        self.conn = None
        self._get_connection()

    @classmethod
    def _initialize_pool(cls):
        with cls._pool_lock:
            if cls._pool is None:
                try:
                    logging.info("Creating database connection pool")
                    cls._pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=2,
                        maxconn=10,
                        host=os.getenv("DB_HOST"),
                        dbname=os.getenv("DB_NAME"),
                        user=os.getenv("DB_USER"),
                        password=os.getenv("DB_PASSWORD"),
                        port=int(os.getenv("DB_PORT", 5432)),
                        sslmode="require",
                        connect_timeout=10,
                        keepalives=1,           # <-- NEW: OS-level TCP keepalives
                        keepalives_idle=30,     #     start probing after 30s idle
                        keepalives_interval=10, #     probe every 10s
                        keepalives_count=5,     #     drop after 5 failed probes
                    )
                    logging.info("Connection pool created successfully")
                except Exception as e:
                    logging.error(f"Failed to create connection pool: {e}")
                    raise

    def _get_connection(self):
        if not self.conn or self.conn.closed:
            self.conn = Database._pool.getconn()
            logging.debug("Retrieved connection from pool")

    def _release_connection(self):
        if self.conn:
            # putconn accepts closed connections; it will discard them
            Database._pool.putconn(self.conn)
            self.conn = None
            logging.debug("Released connection to pool")

    def _ensure_connection(self, max_retries: int = 2):
        """
        Validate the current connection with a lightweight ping.
        If it's dead, release it and grab a fresh one from the pool.
        Retries up to max_retries times.
        """
        for attempt in range(1, max_retries + 1):
            try:
                # Use a fresh cursor just for the ping
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return  # connection is alive
            except _STALE_CONN_ERRORS as e:
                logging.warning(
                    f"Stale connection detected (attempt {attempt}/{max_retries}): {e}"
                )
                try:
                    self._release_connection()
                except Exception:
                    pass
                time.sleep(0.5 * attempt)  # brief back-off
                self._get_connection()

        # Final check after retries
        if not self.is_connected():
            raise psycopg2.OperationalError(
                "Could not obtain a live connection after retries"
            )

    @contextmanager
    def transaction(self):
        self._ensure_connection()
        try:
            yield
            self.conn.commit()
            logging.debug("Transaction committed")
        except Exception as e:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            logging.error(f"Transaction failed, rolled back: {e}")
            raise

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        self._ensure_connection()
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logging.error(f"Error in fetch_one: {e}", exc_info=True)
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            return None

    def fetch_all(self, query: str, params: tuple = None) -> List[Dict]:
        self._ensure_connection()
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logging.error(f"Error in fetch_all: {e}", exc_info=True)
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            return []

    def execute(self, query: str, params: tuple = None, commit: bool = True) -> bool:
        self._ensure_connection()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params or ())
                if commit:
                    self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error executing query: {e}", exc_info=True)
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            return False

    def insert(self, table: str, data: Any, conflict_column: Optional[str] = None) -> bool:
        self._ensure_connection()
        try:
            if table not in self.ALLOWED_TABLES:
                raise ValueError(f"Table '{table}' not in allowed list")

            if isinstance(data, dict):
                data = [data]
            if not data:
                logging.warning("No data provided for insert")
                return False

            columns_list = list(data[0].keys())
            columns = ", ".join(f'"{c}"' for c in columns_list)
            placeholders = ", ".join(["%s"] * len(columns_list))
            conflict_clause = (
                f'ON CONFLICT ("{conflict_column}") DO NOTHING' if conflict_column else ""
            )
            query = f'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) {conflict_clause}'

            values_list = []
            for i, row in enumerate(data, start=1):
                if set(row.keys()) != set(data[0].keys()):
                    raise ValueError(
                        f"Row {i} schema mismatch. "
                        f"Expected: {set(data[0].keys())}, Got: {set(row.keys())}"
                    )
                values_list.append(tuple(row[col] for col in columns_list))

            with self.conn.cursor() as cursor:
                execute_batch(cursor, query, values_list, page_size=100)
            self.conn.commit()
            logging.info(f"Inserted {len(values_list)} row(s) into {table}")
            return True

        except Exception as e:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            logging.error(f"Error inserting into {table}: {e}", exc_info=True)
            return False

    def update(self, table: str, id: Any, timestamp: Any) -> bool:
        self._ensure_connection()
        try:
            if table not in self.ALLOWED_TABLES:
                raise ValueError(f"Table '{table}' not in allowed list")

            query = f'UPDATE "{table}" SET last_scraped = %s WHERE id = %s'
            with self.conn.cursor() as cursor:
                cursor.execute(query, (timestamp, id))
            self.conn.commit()
            logging.info(f"Updated {table} for id={id}")
            return True

        except Exception as e:
            logging.error(f"Error updating {table}: {e}", exc_info=True)
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            return False

    def execute_batch(self, query: str, data: List[tuple]) -> bool:
        self._ensure_connection()
        try:
            if not data:
                logging.warning("No data provided for batch execution")
                return False

            with self.conn.cursor() as cursor:
                execute_batch(cursor, query, data, page_size=100)
            self.conn.commit()
            logging.info(f"Batch execution successful: {len(data)} row(s)")
            return True

        except Exception as e:
            logging.error(f"Batch execution failed: {e}", exc_info=True)
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            return False

    def is_connected(self) -> bool:
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception:
            return False

    def reconnect(self) -> bool:
        try:
            self._release_connection()
            self._get_connection()
            return self.is_connected()
        except Exception as e:
            logging.error(f"Reconnection failed: {e}")
            return False

    def close(self):
        self._release_connection()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @classmethod
    def close_all_connections(cls):
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
            logging.info("All database connections closed")
