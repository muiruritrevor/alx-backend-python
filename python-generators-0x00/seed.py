import mysql.connector
import csv
import uuid
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
    'user' : 'root',
    'password' : 'tking'
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
        mycursor.execute("USE ALX_prodev")

    except Error as e:
        logging.error(f"Error creating database: {e}" )
        raise

    finally:
        mycursor.close()
    

def create_table(connection):
    """Create user_data table with specified schema"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_data (
        user_id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
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


def read_data_from_csv(csv_file):
    """Read CSV file and insert data to user_data table"""
    try:
        with open('user_data.csv', mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                yield row
    
    except FileNotFoundError:
        logger.error(f"CSV file not found {csv_file}")
        raise
    except Exception as e:
        logger.error(f"error reading CSV: {e}")
        raise

def insert_data_from_csv(connection, csv_file):
    """Read CSV file and insert data into user_data table."""
    try:
        cursor = connection.cursor()
        for row in read_data_from_csv(csv_file):
            user_id = row.get('user_id') or str(uuid.uuid4())
            name = row['name']
            email = row['email']
            age = float(row['age'])

            insert_query = """
            INSERT INTO user_data (user_id, name, email, age)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email), age=VALUES(age)
            """
            cursor.execute(insert_query, (user_id, name, email, age))
            logger.info(f"Inserted/Updated user: {name}")

        connection.commit()
        logger.info("All data inserted successfully")
    except Error as e:
        logger.error(f"Error inserting data: {e}")
        raise
    except KeyError as e:
        logger.error(f"Missing column in CSV: {e}")
        raise
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file}")
        raise
    finally:
        cursor.close()


def main():
    """Main function to orchestrate database setup and seeding."""
    try:
        # Connect to MySQL
        connection = connect_to_mysql()
        
        # Create database and table
        create_database(connection)
        create_table(connection)
        
        # Insert data from CSV
        insert_data_from_csv(connection, csv_file)
        
        logger.info("Database seeding completed successfully")
    except Exception as e:
        logger.error(f"Script failed: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            logger.info("MySQL connection closed")

if __name__ == "__main__":
    main()
