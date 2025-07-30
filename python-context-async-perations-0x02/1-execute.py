import asnycio
import sqlite3

asycio def execute_query(self, query):
        """Asynchronously execute a query on the database."""
        async with DatabaseConnection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return cursor.fetchall()
        # Return False to propagate exceptions, True to suppress them
        return False