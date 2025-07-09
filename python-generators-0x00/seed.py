import os
import mysql.connector
import csv
from uuid import uuid4
import logging

from mysql.connector import Error
 
# configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host' : 'localhost',
    'user' : os.environ.get("DB_USER"),
    'password' : os.environ.get("DB_PASS")
}

# CSV file path
csv_file = 'user_data.csv'

def connect_to_mysql():
    """Connect to MySQL server and return connection object."""
    
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        logging.info("Successfully connected to MySQLServer")
        return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL {e}")
        raise


def create_database(connection):
    """Create ALX_prodev database if it does not exit."""

    try:
        mycursor = connection.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTs ALX_prodev")
        logging.info("Database ALX_prodev created or already exists")

    except Error as e:
        logging.error(f"Error creating database: {e}" )
        raise

    finally:
        mycursor.close()


def connect_to_db():
    """Connect directly to ALX_prodev database"""

    try:
        db_config = DB_CONFIG.copy()
        db_config["database"] = "ALX_prodev"
        connection = mysql.connector.connect(**db_config)
        logging.info("Connected to ALX_prodev database")
        return connection
    
    except Error as e:
        logging.ERROR(f"Error connecting to ALX_prodev database {e}")
        return None

    

def create_table(connection):
    """Create user_data table with specified schema"""
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_data (
        user_id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE,
        age DECIMAL(5,2) NOT NULL,
        INDEX idx_user_id (user_id)
    )
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
            logger.info("Table user_data created or already exists")
    except Error as e:
        logger.error(f"Error creating table: {e}")
        raise
    finally:
         cursor.close()


def insert_data(connection, data):
    """Insert data from CSV into user_data table, skipping duplicates."""

    try:
        with open(data, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            cursor = connection.cursor()
            inserted = 0

            for row in reader:
                user_id = str(uuid4())
                name = row['name']
                email = row['email']
                age = int(row['age'])

                cursor.execute(
                    "INSERT IGNORE INTO user_data (user_id, name, email, age) VALUES (%s, %s, %s, %s)",
                    (user_id, name, email, age)
                )
                if cursor.rowcount:
                    inserted += 1

            connection.commit()
            logging.info(f"{inserted} rows inserted (duplicates skipped).")

    except Error as e:
        logging.error(f"MySQL error: {e}")
    except FileNotFoundError:
        logging.error("CSV file not found.")
    except KeyError as e:
        logging.error(f"Missing column in CSV: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if cursor:
            cursor.close()

