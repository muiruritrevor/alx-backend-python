import functools
import sqlite3


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


query_cache = {}


def cache_query(func):
    """
    Decorator to cache the results of a function based on its arguments.
    
    :param func: The function to be cached.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Create a unique key for the cache based on function name and arguments
        key = (func.__name__, args, frozenset(kwargs.items()))
        if key in query_cache:
            print("Returning cached result.")
            return query_cache[key]
        
        print("Executing query and caching result.")
        result = func(*args, **kwargs)
        query_cache[key] = result
        return result
    
    return wrapper


@with_db_connection
@cache_query
def fetch_users_with_cache(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

# First call will cache the result
users = fetch_users_with_cache(query="SELECT * FROM users")
     
# Second call will use the cached result
users_again = fetch_users_with_cache(query="SELECT * FROM users")