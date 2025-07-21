from seed import connect_to_db
from mysql.connector import Error
import logging

# Configure logging to display time, log level, and message
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def paginate_users(page_size, offset):
    """
    Fetch a single page of users from the database.
    
    Args:
        page_size (int): Number of users per page.
        offset (int): Offset to start retrieving records from.
    
    Returns:
        list[dict]: List of user records.
    """
    connection = connect_to_db()
    if not connection:
        logger.error("Failed to connect to database")
        return []

    cursor = connection.cursor(dictionary=True, buffered=True)
    try:
        query = "SELECT * FROM user_data LIMIT %s OFFSET %s"
        cursor.execute(query, (page_size, offset))
        rows = cursor.fetchall()
        return rows

    except Error as e:
        logger.error(f"Error fetching rows: {e}")
        return []

    finally:
        cursor.close()
        connection.close()
        logger.info("Database connection closed")


def lazy_paginate(page_size):
    """
    Generator that lazily yields user records page by page.

    Args:
        page_size (int): Number of users per page.

    Yields:
        dict: Individual user records.
    """
    offset = 0
    while True:
        page = paginate_users(page_size, offset)
        if not page:
            break  # No more records
        for user in page:
            yield user
        offset += page_size


# Entry point for script execution
if __name__ == "__main__":
    for user in lazy_paginate(100):
        print(user)
