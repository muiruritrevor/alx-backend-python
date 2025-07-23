import sqlite3
import functools
from datetime import datetime


def log_queries(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        query = args[0] if args else kwargs.get('query')
        date = datetime.now()
        if query:
            print(f"Executing query: {query} on {date}")
        return func(*args, **kwargs)
    return wrapper


@log_queries
def fetch_all_users(query):
    connection = sqlite3.connect('users.db')
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    connection.close()
    return results


# Fetch users while logging the query
users = fetch_all_users(query="SELECT * FROM users")