import sqlite3


class DatabaseConnection:
    """A class to manage database connections.
    This class provides methods to connect to and close a database connection.
    """
    def __init__(self):
        self.connection = None

    def __enter__(self):
        self.connection=sqlite3.connect('users.db')
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()
        # Return False to propagate exceptions, True to suppress them
        return False  

with DatabaseConnection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    conn.commit()
    print(cursor.fetchall())