# from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query

import os
from dotenv import load_dotenv
from databricks import sql


def test_load(capsys):
    '''Test the load function'''

    test1 = load()
    assert test1 == "Data loaded or has been loaded successfully"
    captured = capsys.readouterr()
    assert "Data loaded and splited successfully" in captured.out

    # from .env, load the databricks connection details
    load_dotenv()
    # Connect to the databricks database
    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                    http_path       = os.getenv("HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_KEY")) as connection:
        # SQL commands
        with connection.cursor() as cursor:

            cursor.execute("SELECT * FROM hy220_nasdaq_aapl_price")
            result = cursor.fetchall()
            assert result is not None

            cursor.execute("SELECT * FROM hy220_nasdaq_aapl_volume")
            result = cursor.fetchall()
            assert result is not None

            cursor.close()
        connection.close()

def test_query(capsys):
    '''Test the query function'''
    test2 = query()
    assert test2 == "Data queried and saved successfully"
    captured = capsys.readouterr()
    assert "Date=datetime.date(2010, 1, 5)" in captured.out

    # from .env, load the databricks connection details
    load_dotenv()
    with sql.connect(server_hostname=os.getenv("SERVER_HOSTNAME"),
                     http_path=os.getenv("HTTP_PATH"),
                     access_token=os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:

            cursor.execute("SELECT * FROM hy220_nasdaq_aapl_total")
            result = cursor.fetchall()
            assert result is not None

            cursor.close()
        connection.close()

# run this test by 
# pytest test_main.py
