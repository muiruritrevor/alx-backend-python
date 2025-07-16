"""
A generator to fetch and process data in batches from the usALX_prodev database

"""

import seed


def stream_users_in_batches(batch_size):
    """
    A Generator function that streams rows from the 'user_data' table in batches.
    """
    try:
        cursor = connection.cursor(dictonary=True, buffered=True)
        cursor.execute("SELECT * FROM User_data")
        for row in cursor:
            yield row
    except Exception as e:
        print(f"Error yielding rows {e}")
