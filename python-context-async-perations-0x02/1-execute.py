import sqlite3

# class ExecuteQuery(query):
#     def __init__(self):
#         self.connection = None
#         self.query = query
    
#     def __enter__(self):
#         self.connection = sqlite3.connect('users.db')
#         cursor = self.connection.cursor()
#         cursor.execute(self.query)
#         self.connection.commit()
#         print(cursor.fetchall())
#         return self.connection
    
#     def __exit__(self, exc_type, exc_value, traceback):
#         if self.connection:
#             self.connection.close()
#         # Return False to propagate exceptions, True to suppress them
#         return False

class ExecuteQuery