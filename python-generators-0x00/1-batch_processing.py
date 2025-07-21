from seed import connect_to_db
from mysql.connector import Error
import logging

# Configure logging to display time, log level, and message
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def stream_users_in_batches(batch_size: int):
    """
    Generator that streams user records from the 'user_data' table in batches.
    
    Args:
        batch_size (int): Number of records to retrieve per batch.
        
    Yields:
        list[dict]: A batch of user records as dictionaries.
    """
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("Batch size must be a positive integer")

    # Establish database connection
    connection = connect_to_db()
    if not connection:
        logger.error("Failed to connect to database")
        return

    # Use buffered cursor to allow multiple fetches
    cursor = connection.cursor(dictionary=True, buffered=True)
    try:
        # SQL query to fetch all user records
        query = "SELECT * FROM user_data"
        cursor.execute(query)

        # Loop to fetch data in batches
        while True:
            batch = cursor.fetchmany(size=batch_size)
            if not batch:
                break  # Exit when no more records are available
            yield batch  # Return the current batch
    except Error as e:
        logger.error(f"Error streaming batch: {e}")
    finally:
        # Always close cursor and connection
        cursor.close()
        connection.close()
        logger.info("Database connection closed")


def batch_processing(batch_size: int):
    """
    Processes batches of user data and prints users over the age of 25.
    
    Args:
        batch_size (int): Number of records to process per batch.
    """
    try:
        # Iterate over streamed batches
        for batch in stream_users_in_batches(batch_size):
            # Filter users older than 25
            eligible_users = [user for user in batch if user.get("age", 0) > 25]

            # Process each eligible user
            for user in eligible_users:
                print(f"{user}")

            logger.info(f"Processed batch with {len(eligible_users)} users over 25")
    except Exception as e:
        logger.error(f"Error processing batches: {e}")


# Entry point for script execution
if __name__ == "__main__":
    batch_size = 50
    batch_processing(batch_size)
