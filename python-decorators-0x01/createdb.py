import sqlite3


def create_database(db_name):

    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(f"{db_name}.db")
    
    # Create a cursor object using the cursor() method
    cursor = conn.cursor()
    
    # Create a sample table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            age INTEGER NOT NULL

        )
    ''')
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    
    print(f"Database '{db_name}.db' created successfully with a sample table.")


def insert_sample_data(db_name):

    # Connect to the SQLite database
    conn = sqlite3.connect(f"{db_name}.db")
    
    # Create a cursor object using the cursor() method
    cursor = conn.cursor()
    
    # Insert sample data into the users table
    sample_data = [
        ('Alice', 'alice@doe.com',30),
        ('Bob', 'bob@doe.com', 25),
        ('Charlie', 'charlie@doe.com', 35)
    ]
    
    cursor.executemany('''
        INSERT INTO users (name, email, age) VALUES (?, ?, ?)
    ''', sample_data)
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    
    print("Sample data inserted successfully.") 

# Example usage
if __name__ == "__main__":
    create_database("users")
    insert_sample_data("users")