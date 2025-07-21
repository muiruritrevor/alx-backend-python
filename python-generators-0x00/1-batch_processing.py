from seed import connect_to_db
from mysql.connector import Error
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def stream_users_in_batches(batch_size: int):
    """
    Generator function that streams user records from the 'user_data' table in batches.
    Args:
        batch_size (int): Number of rows per batch.
    Yields:
        list: A batch of user records (dictionaries).
    """
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("Batch size must be a positive integer")

    connection = connect_to_db()
    if not connection:
        logger.error("Failed to connect to database")
        return

    cursor = connection.cursor(dictionary=True, buffered=True)
    try:
        # Query to filter users over the age of 25 (Loop 1: Fetch batches)
        query = "SELECT * FROM user_data"
        cursor.execute(query)
        while True:
            batch = cursor.fetchmany(size=batch_size)
            if not batch:
                break
            yield batch
    except Error as e:
        logger.error(f"Error streaming batch: {e}")
    finally:
        cursor.close()
        connection.close()
        logger.info("Database connection closed")


def batch_processing(batch_size: int):
    """
    Process batches of user data, printing users over 25.
    Args:
        batch_size (int): Number of rows per batch.
    """
    try:
        # Loop 2: Iterate over batches
        for batch in stream_users_in_batches(batch_size):
            process = [user for user in batch if user.get("age", 0) > 25]
            # Loop 3: Process rows in batch
            for user in process:
                print(f"{user}")
            logger.info(f"Processed batch with {len(batch)} users over 25")
    except Exception as e:
        logger.error(f"Error processing batches: {e}")

# Run the process
if __name__ == "__main__":
    batch_size = 50
    batch_processing(batch_size)