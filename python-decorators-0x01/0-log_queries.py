"""
A decorator that logs database queries executed by any function
"""

import sqlite3
import functools


def log_queries(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        query = kwargs.get('query', None)
        if query:
            print(f"Executing query: {query}")
        return func(*args, **kwargs)
    return wrapper
    


def fetch_all_users(query):
    connection = sqlite3.connect('users.db')
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    connection.close()
    return results

# Fetch users while logging the query

users = fetch_all_users(query="SELECT * FROM users")