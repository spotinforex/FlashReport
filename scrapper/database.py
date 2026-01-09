import logging, os, sys
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

load_dotenv()

class Database:
    """
    Handles All Database Connections 
    """
    def __init__(self):
        try:
            logging.info("Connecting To Database")
            self.conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                port=int(os.getenv("DB_PORT", 5432)),
                sslmode="require"
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logging.info("Connected To Database")
        except Exception as e:
            logging.error(f"Failed To Connect to Database. Error: {e}")
            self.conn = None
            self.cursor = None

    def fetch_one(self, query, params=None):
        """
        Execute a query and return a single record
        """
        try:
            self.cursor.execute(query, params or ())
            return self.cursor.fetchone()
        except Exception as e:
            logging.error(f"Error fetching one: {e}")
            return None

    def fetch_all(self, query, params=None):
        """
        Execute a query and return all records
        """
        try:
            self.cursor.execute(query, params or ())
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(f"Error fetching all: {e}")
            return []

    def execute(self, query, params=None, commit=True):
        """
        Execute a query (INSERT, UPDATE, DELETE)
        """
        try:
            self.cursor.execute(query, params or ())
            if commit:
                self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            self.conn.rollback()
            return False
            
    def insert(self, table, data, conflict_column = None):
        """
        Insert one or many records into a table.
        `data` can be:
            - a single dictionary: {column: value}
            - a list of dictionaries: [{...}, {...}]
        """
        try:
            # Normalize input
            if isinstance(data, dict):
                data = [data]

            if not data:
                logger.warning("No data provided for insert")
                return False

            # Freeze column order from first row
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

            values_list = []

            for i, row in enumerate(data, start=1):
                # Enforce schema consistency
                if row.keys() != data[0].keys():
                    raise ValueError(
                        f"Row {i} keys mismatch.\n"
                        f"Expected: {data[0].keys()}\n"
                        f"Got: {row.keys()}"
                    )

                values_list.append(
                    tuple(row[col] for col in columns_list)
                )

            # Batch insert (fast & safe)
            execute_batch(self.cursor, query, values_list, page_size=100)
            self.conn.commit()

            logger.info(f"Inserted {len(values_list)} row(s) into {table}")
            return True

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting into {table}: {e}", exc_info=True)
            return False
            
    def update(self, table, id, timestamp):
        """Updates Timestamp For Sources safely"""
        try:
            logging.info("Updating Source Table Scrape Date In Progress")

            query = f"UPDATE {table} SET last_scraped = %s WHERE id = %s"
            self.cursor.execute(query, (timestamp, id))

            self.conn.commit()
            logging.info("Updated Table Successfully")

        except Exception as e:
            logging.error(f"An Error Occurred While Updating Table: {e}")

    def execute_batch(self, query, data):
        try:
            logging.info("Batch Insertion In Progress")
            # Batch insert (fast & safe)
            execute_batch(self.cursor, query, data, page_size=100)
            self.conn.commit()

            logger.info(f"Inserted {len(data)} row(s)")
            return True
        except Exception as e:
            logging.error(f"An Error Occurred While Batch Inserting Table: {e}")
        
    def close(self):
        """
        Close cursor and connection
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("Database connection closed")

    

