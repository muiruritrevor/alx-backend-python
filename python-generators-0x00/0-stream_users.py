"""
A generator that streams rows from an SQL database one by one.

The function uses a generator to fetch rows one by one from the user_data table

"""


import seed


def stream_rows(connection):
        """Generatoe function to stream rows from user_data table one by one"""
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("Select * From user_data")
            for row in cursor:
                  yield row

        except Exception as e:
              print(f"Error yielding rows {e}")
            
        finally:
              cursor.close()
