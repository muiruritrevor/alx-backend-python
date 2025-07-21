from seed import connect_to_db
from mysql.connector import Error
import logging

# Configure logging to display time, log level, and message
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def stream_user_ages():
    connection = connect_to_db()
    if not connection:
        logger.error("Failed to connect to database")
        return []

    cursor = connection.cursor(dictionary=True, buffered=True)
    try:
        query = "SELECT age FROM user_data"
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            yield row['age']

    except Error as e:
        logger.error(f"Error fetching rows: {e}")
        return []

    finally:
        cursor.close()
        connection.close()
        logger.info("Database connection closed")


def avg_age():
    total = 0
    count = 0

    for age in stream_user_ages():
        total += age
        count += 1

    if count > 0:
        average = total / count
        print(f"Average age of users: {average}")
    else:
        print("No users found to calculate average age.")


if __name__=="__main__" :
    avg_age()
