"""Query the database"""

import os
from dotenv import load_dotenv
from databricks import sql

def query():
    """Query the database for the top 2 rows of the nasdaq_aapl table"""

# from .env, load the databricks connection details
    load_dotenv()

    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                    http_path       = os.getenv("HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:
            query = "SELECT * FROM nasdaq_aapl LIMIT 2"
            cursor.execute(query)

            # Print the result
            result = cursor.fetchall()
            for row in result:
                print(row)

            cursor.close()
        connection.close()

if __name__ == "__main__":
    query()
