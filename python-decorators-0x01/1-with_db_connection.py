import sqlite3
import functools

def with_db_connection(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        connection = sqlite3.connect('users.db')
        try:
            result = func(connection, *args, **kwargs)
            return result
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            connection.close()
    return wrapper


@with_db_connection
def get_user_by_id(connection, user_id):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    return cursor.fetchone()

# Fetch a user by ID with a database connection
user = get_user_by_id(user_id=1)
print(user)