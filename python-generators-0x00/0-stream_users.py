"""
Module for streaming rows from the `user_data` table using a generator pattern.

This module establishes a database connection using the `seed` module, streams
records from the `user_data` table row by row, and logs each row as it is retrieved.
It includes basic error handling and logging for monitoring purposes.
"""

import seed
import logging
from itertools import islice

# Configuring logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def stream_users():
    pass

def stream_users(connection):
    """
    Generator function that streams rows from the 'user_data' table one at a time.

    Parameters:
        connection: A valid database connection object from the `seed` module.

    Yields:
        dict: A dictionary representing a single row from the 'user_data' table.

    Raises:
        seed.Error: If an error occurs during query execution or iteration.
    """
    try:
        cursor = connection.cursor(dictionary=True, buffered=True)
        cursor.execute("SELECT * FROM user_data")
        for row in cursor:
            logger.info(f"Streaming row: {row}")
            yield row

    except seed.Error as e:
        logger.info(f"Error yielding rows: {e}")
        raise

    finally:
        cursor.close()


# Test the Generator
if __name__ == "__main__":
    connection = seed.connect_to_db()
    if connection:
        try:
            for row in islice(stream_users(connection), 6):
                print(row)
        except seed.Error as e:
            logger.error(f"Error in test: {e}")
            raise
        finally:
            connection.close()