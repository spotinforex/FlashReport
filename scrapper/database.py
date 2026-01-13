import logging
import os
import sys
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


class Database:
    """
    Thread-safe database connection handler with connection pooling
    """
    
    _pool = None
    _pool_lock = __import__('threading').Lock()
    
    # Whitelist of allowed tables for dynamic queries
    ALLOWED_TABLES = {
        'sources', 'parsed_articles', 'signals', 
        'events', 'event_articles', 'analysis'
    }

    def __init__(self):
        """Initialize database connection from pool"""
        if Database._pool is None:
            self._initialize_pool()
        
        self.conn = None
        self._get_connection()

    @classmethod
    def _initialize_pool(cls):
        """Create connection pool (singleton pattern)"""
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
                        connect_timeout=10
                    )
                    logging.info("Connection pool created successfully")
                except Exception as e:
                    logging.error(f"Failed to create connection pool: {e}")
                    raise

    def _get_connection(self):
        """Get connection from pool"""
        if not self.conn or self.conn.closed:
            self.conn = Database._pool.getconn()
            logging.debug("Retrieved connection from pool")

    def _release_connection(self):
        """Return connection to pool"""
        if self.conn and not self.conn.closed:
            Database._pool.putconn(self.conn)
            self.conn = None
            logging.debug("Released connection to pool")

    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions
        """
        try:
            yield
            self.conn.commit()
            logging.debug("Transaction committed")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Transaction failed, rolled back: {e}")
            raise

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        """
        Execute query and return single record as dict
        
        Args:
            query: SQL query with placeholders
            params: Query parameters
            
        Returns:
            Dictionary of results or None
        """
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logging.error(f"Error in fetch_one: {e}", exc_info=True)
            self.conn.rollback()
            return None

    def fetch_all(self, query: str, params: tuple = None) -> List[Dict]:
        """
        Execute query and return all records as list of dicts
        
        Args:
            query: SQL query with placeholders
            params: Query parameters
            
        Returns:
            List of dictionaries
        """
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logging.error(f"Error in fetch_all: {e}", exc_info=True)
            self.conn.rollback()
            return []

    def execute(self, query: str, params: tuple = None, commit: bool = True) -> bool:
        """
        Execute query (INSERT, UPDATE, DELETE)
        
        Args:
            query: SQL query
            params: Query parameters
            commit: Whether to commit immediately
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params or ())
                if commit:
                    self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error executing query: {e}", exc_info=True)
            self.conn.rollback()
            return False

    def insert(
        self, 
        table: str, 
        data: Any, 
        conflict_column: Optional[str] = None
    ) -> bool:
        """
        Insert one or multiple records into table
        
        Args:
            table: Table name
            data: Single dict or list of dicts
            conflict_column: Column for ON CONFLICT clause
            
        Returns:
            True if successful
        """
        try:
            # Validate table name
            if table not in self.ALLOWED_TABLES:
                raise ValueError(f"Table '{table}' not in allowed list")
            
            # Normalize input
            if isinstance(data, dict):
                data = [data]

            if not data:
                logging.warning("No data provided for insert")
                return False

            # Get column order from first row
            columns_list = list(data[0].keys())
            columns = ", ".join(f'"{c}"' for c in columns_list)
            placeholders = ", ".join(["%s"] * len(columns_list))

            conflict_clause = ""
            if conflict_column:
                conflict_clause = f'ON CONFLICT ("{conflict_column}") DO NOTHING'

            query = f"""
                INSERT INTO "{table}" ({columns})
                VALUES ({placeholders})
                {conflict_clause}
            """

            # Validate schema consistency
            values_list = []
            for i, row in enumerate(data, start=1):
                if set(row.keys()) != set(data[0].keys()):
                    raise ValueError(
                        f"Row {i} schema mismatch. "
                        f"Expected: {set(data[0].keys())}, Got: {set(row.keys())}"
                    )
                values_list.append(tuple(row[col] for col in columns_list))

            # Batch insert
            with self.conn.cursor() as cursor:
                execute_batch(cursor, query, values_list, page_size=100)
            
            self.conn.commit()
            logging.info(f"Inserted {len(values_list)} row(s) into {table}")
            return True

        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error inserting into {table}: {e}", exc_info=True)
            return False

    def update(self, table: str, id: Any, timestamp: Any) -> bool:
        """
        Update last_scraped timestamp for a record
        
        Args:
            table: Table name
            id: Record ID
            timestamp: New timestamp
            
        Returns:
            True if successful
        """
        try:
            # Validate table name
            if table not in self.ALLOWED_TABLES:
                raise ValueError(f"Table '{table}' not in allowed list")
            
            logging.info(f"Updating {table} scrape date for id={id}")
            
            query = f'UPDATE "{table}" SET last_scraped = %s WHERE id = %s'
            
            with self.conn.cursor() as cursor:
                cursor.execute(query, (timestamp, id))
            
            self.conn.commit()
            logging.info("Update successful")
            return True
            
        except Exception as e:
            logging.error(f"Error updating {table}: {e}", exc_info=True)
            self.conn.rollback()
            return False

    def execute_batch(self, query: str, data: List[tuple]) -> bool:
        """
        Execute batch operations efficiently
        
        Args:
            query: SQL query with placeholders
            data: List of tuples containing parameters
            
        Returns:
            True if successful
        """
        try:
            if not data:
                logging.warning("No data provided for batch execution")
                return False
            
            logging.info(f"Batch executing {len(data)} operations")
            
            with self.conn.cursor() as cursor:
                execute_batch(cursor, query, data, page_size=100)
            
            self.conn.commit()
            logging.info(f"Batch execution successful: {len(data)} row(s)")
            return True
            
        except Exception as e:
            logging.error(f"Batch execution failed: {e}", exc_info=True)
            self.conn.rollback()
            return False

    def is_connected(self) -> bool:
        """Check if database connection is alive"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except:
            return False

    def reconnect(self) -> bool:
        """Reconnect to database"""
        try:
            logging.info("Attempting to reconnect")
            self._release_connection()
            self._get_connection()
            
            if self.is_connected():
                logging.info("Reconnection successful")
                return True
            return False
            
        except Exception as e:
            logging.error(f"Reconnection failed: {e}")
            return False

    def close(self):
        """Release connection back to pool"""
        self._release_connection()
        logging.info("Database connection closed")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto cleanup"""
        self.close()

    @classmethod
    def close_all_connections(cls):
        """Close all connections in pool (call on shutdown)"""
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
            logging.info("All database connections closed")
