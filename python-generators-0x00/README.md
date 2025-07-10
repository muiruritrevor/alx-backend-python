# ALX_prodev Database Seeder and Row Streaming Generator

## Overview
`seed.py` sets up a MySQL database (`ALX_prodev`) with a `user_data` table, populates it from `user_data.csv`. It includes error handling and logging.

## Features
- Creates `ALX_prodev` database and `user_data` table (`user_id` UUID, `name`, `email`, `age`).
- Inserts CSV data, skipping duplicates (unique `email`).
- Logs operations and errors.

## Prerequisites
- Python 3.6+
- MySQL Server
- `mysql-connector-python` (`pip install mysql-connector-python`)
- `user_data.csv` with `name`, `email`, `age`
- Environment variables: `DB_USER`, `DB_PASS`

## Installation
1. Install dependencies:
   ```bash
   pip install mysql-connector-python

2. Set environment variables
    ```bash
    export DB_USER='your_username'
    export DB_PASS='your_password'

3. Add user_data.csv:
   ```csv
    name,email,age
    John Doe,john.doe@example.com,30

## Usage
### Populate database
    python seed.py


# CSV Format
    name,email,age
    Alice Johnson,alice.johnson@example.com,28

### Notes

- email is unique; user_id is UUID.
- Uses INSERT IGNORE for duplicates.
- Logs to console.

### Limitations

- UTF-8 CSV encoding required.
- DB_USER, DB_PASS must be set.
- Basic CSV parsing.

### License
- MIT License