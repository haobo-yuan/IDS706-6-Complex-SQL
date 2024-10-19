import os
from dotenv import load_dotenv
from databricks import sql

def query():
    """Query the database, perform JOIN, aggregation, sorting, and save results to a new table"""

    # from .env, load the databricks connection details
    load_dotenv()

    with sql.connect(server_hostname=os.getenv("SERVER_HOSTNAME"),
                     http_path=os.getenv("HTTP_PATH"),
                     access_token=os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:

            # create a new table to save the JOINed results
            create_total_table_query = """
            CREATE TABLE IF NOT EXISTS hy220_nasdaq_aapl_total AS
            SELECT 
                p.Date, 
                p.Open, 
                p.High, 
                p.Low, 
                p.Close, 
                p.Adj_Close, 
                v.Volume, 
                p.Name,
                AVG(p.Close) OVER (PARTITION BY p.Name ORDER BY p.Date) AS avg_close_price
            FROM hy220_nasdaq_aapl_price p
            JOIN hy220_nasdaq_aapl_volume v
            ON p.Date = v.Date AND p.Name = v.Name
            ORDER BY v.Volume DESC, p.Date
            """
            cursor.execute(create_total_table_query)

            # check the new table
            check_total_table_query = "SELECT * FROM hy220_nasdaq_aapl_total LIMIT 5"
            cursor.execute(check_total_table_query)
            result = cursor.fetchall()
            for row in result:
                print(row)

            cursor.close()
        connection.close()

    return "Data queried and saved successfully"

if __name__ == "__main__":
    query()
