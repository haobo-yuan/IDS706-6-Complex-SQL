import os
from dotenv import load_dotenv
from databricks import sql
import pandas as pd

#load the csv file and insert into a new databricks database
def load(dataset="./data/NASDAQ_100_Data_From_2010.csv"):
    """Transforms and Loads data into the databricks database"""

    # Load the data through pandas
    df = pd.read_csv(dataset, nrows=5, sep='\t')
    df.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)  # Rename the column with space
    print(df)

    # from .env, load the databricks connection details
    load_dotenv()

    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                    http_path       = os.getenv("HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:

            # Create the table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS nasdaq_aapl (
                Date STRING,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                Close DOUBLE,
                Adj_Close DOUBLE,
                Volume BIGINT,
                Name STRING
            )
            """
            cursor.execute(create_table_query)

            # Convert the DataFrame to SQL statements for multi-row insert
            rows = []
            for index, row in df.iterrows():
                rows.append(f"('{row['Date']}', {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Adj_Close']}, {row['Volume']}, '{row['Name']}')")

            # Insert the data
            insert_query = f"""
            INSERT INTO nasdaq_aapl (Date, Open, High, Low, Close, Adj_Close, Volume, Name) VALUES
            {", ".join(rows)}
            """
            cursor.execute(insert_query)

            cursor.close()
        connection.close()

if __name__ == "__main__":
    load()