import sqlite3

class ExecuteQuery:
    """A reusable context manager to execute SQL queries and manage database connections."""
    def __init__(self, query, params=None):
        self.query = query
        self.params = params if params is not None else ()
        self.connection = None
        self.result = None

    def __enter__(self):
        self.connection = sqlite3.connect('users.db')
        cursor = self.connection.cursor()
        cursor.execute(self.query, self.params)
        self.result = cursor.fetchall()
        return self.result

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()
        return False  # Propagate any exception if it occurs


query = "SELECT * FROM users WHERE age > ?"
params = (25,)

with ExecuteQuery(query, params) as result:
    for row in result:
        print(row)
