import sqlite3
import pandas as pd

def my_load(db_path, csv_path, nrows=None):
    """
    Load data from a CSV file into a SQLite database.

    Parameters:
    - db_path: Path to the SQLite database file.
    - csv_path: Path to the CSV file to be loaded.
    - nrows: Number of rows to read from the CSV file.
    """
    with sqlite3.connect(db_path) as conn:
        # Read data from the CSV file
        df = pd.read_csv(csv_path, nrows=nrows, sep='\t')
        # Drop the 'Adj Close' column if it exists
        if 'Adj Close' in df.columns:
            df = df.drop(columns=['Adj Close'])
        # Write the DataFrame to the 'stocks' table
        df.to_sql('stocks', conn, if_exists='replace', index=False)

def my_create(db_path, record):
    """
    Insert a new record into the 'stocks' table.

    Parameters:
    - db_path: Path to the SQLite database file.
    - record: Tuple containing the record data.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO stocks (Date, Open, High, Low, Close, Volume, Name)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, record)
        conn.commit()

def my_read(db_path, name):
    """
    Read records from the 'stocks' table where Name matches.

    Parameters:
    - db_path: Path to the SQLite database file.
    - name: Name of the stock to read.

    Returns:
    - List of tuples containing the query results.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM stocks WHERE Name = ?", (name,))
        results = cursor.fetchall()
    return results

def my_update(db_path, name, new_close):
    """
    Update the 'Close' value of a record in the 'stocks' table.

    Parameters:
    - db_path: Path to the SQLite database file.
    - name: Name of the stock to update.
    - new_close: New value for the 'Close' field.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE stocks SET Close = ? WHERE Name = ?", (new_close, name))
        conn.commit()

def my_delete(db_path, name):
    """
    Delete records from the 'stocks' table where Name matches.

    Parameters:
    - db_path: Path to the SQLite database file.
    - name: Name of the stock to delete.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stocks WHERE Name = ?", (name,))
        conn.commit()
