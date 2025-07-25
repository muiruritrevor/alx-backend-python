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


def transactional(func):
    @functools.wraps(func)
    def wrapper(connection, *args, **kwargs):
        cursor = connection.cursor()
        try:
            result = func(cursor, *args, **kwargs)
            connection.commit()
            return result
        except Exception as e:
            connection.rollback()
            print(f"Transaction failed: {e}")
        finally:
            cursor.close()
    return wrapper

@with_db_connection
@transactional
def update_user_email(cursor, user_id, new_email):
    cursor.execute("UPDATE users SET email = ? WHERE id = ?", (new_email, user_id))

# Update user's email with automatic transaction handling
update_user_email(user_id=1, new_email='Cartwright@hotmail.com')
print("User's email updated successfully.")