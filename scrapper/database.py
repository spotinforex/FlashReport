import logging, os, sys
import psycopg2
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
            self.cursor = self.conn.cursor()
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
            
    def insert(self, table, data):
        """
        Insert one or many records into a table.
        `data` can be:
            - a single dictionary: {column: value}
            - a list of dictionaries: [{...}, {...}]
        """
        try:
            if isinstance(data, dict):
                data = [data]  # wrap single dict into a list

            if not data:
                logging.warning("No data provided to insert")
                return False

            columns = ', '.join(data[0].keys())
            placeholders = ', '.join(['%s'] * len(data[0]))
            query = f'INSERT INTO "{table}" ({columns}) VALUES ({placeholders})'

            # Insert each row
            for row in data:
                values = tuple(row.values())
                self.cursor.execute(query, values)

            self.conn.commit()
            logging.info(f"Inserted {len(data)} row(s) into {table}")
            return True

        except Exception as e:
            logging.error(f"Error inserting into {table}: {e}")
            self.conn.rollback()
            return False

    def close(self):
        """
        Close cursor and connection
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("Database connection closed")


def preprocessor(data):
    """ 
    Preproceses and Clean data for the Database
    """
    pass
