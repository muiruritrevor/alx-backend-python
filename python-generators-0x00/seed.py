"""
Module to manage MySQL database operations for the ALX_prodev project.

This script connects to a MySQL server, creates a database and table if they don't exist,
and inserts data from a CSV file into the table, skipping duplicate entries.
"""

import os
import mysql.connector
import csv
from uuid import uuid4
import logging
from mysql.connector import Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASS")
}

# CSV file path
csv_file = 'user_data.csv'


def connect_to_mysql():
    """
    Establish a connection to the MySQL server.

    Returns:
        mysql.connector.connection.MySQLConnection: MySQL connection object.

    Raises:
        mysql.connector.Error: If connection fails.
    """
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        logger.info("Successfully connected to MySQL server")
        return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise


def create_database(connection):
    """
    Create the 'ALX_prodev' database if it does not already exist.

    Parameters:
        connection (mysql.connector.connection.MySQLConnection): Connection to MySQL server.

    Raises:
        mysql.connector.Error: If database creation fails.
    """
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        logger.info("Database ALX_prodev created or already exists")
    except Error as e:
        logger.error(f"Error creating database: {e}")
        raise
    finally:
        cursor.close()


def connect_to_db():
    """
    Connect to the 'ALX_prodev' database.

    Returns:
        mysql.connector.connection.MySQLConnection | None: Connection object or None on failure.
    """
    try:
        db_config = DB_CONFIG.copy()
        db_config["database"] = "ALX_prodev"
        connection = mysql.connector.connect(**db_config)
        logger.info("Connected to ALX_prodev database")
        return connection
    except Error as e:
        logger.error(f"Error connecting to ALX_prodev database: {e}")
        return None


def create_table(connection):
    """
    Create the 'user_data' table if it doesn't exist.

    Parameters:
        connection (mysql.connector.connection.MySQLConnection): Connection to the ALX_prodev database.

    Raises:
        mysql.connector.Error: If table creation fails.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_data (
        user_id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE,
        age INT NOT NULL,
        INDEX idx_user_id (user_id)
    )
    """
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        logger.info("Table user_data created or already exists")
    except Error as e:
        logger.error(f"Error creating table: {e}")
        raise
    finally:
        cursor.close()


def insert_data(connection, data):
    """
    Insert user data from a CSV file into the 'user_data' table.

    Skips rows with duplicate email addresses using INSERT IGNORE.

    Parameters:
        connection (mysql.connector.connection.MySQLConnection): Connection to the ALX_prodev database.
        data (str): Path to the CSV file.

    Logs:
        Number of rows inserted, and any errors encountered.
    """
    cursor = None
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
            logger.info(f"{inserted} rows inserted (duplicates skipped).")

    except Error as e:
        logger.error(f"MySQL error: {e}")
    except FileNotFoundError:
        logger.error("CSV file not found.")
    except KeyError as e:
        logger.error(f"Missing column in CSV: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if cursor:
            cursor.close()
