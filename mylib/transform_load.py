import os
from dotenv import load_dotenv
from databricks import sql
import pandas as pd


def load(dataset="./data/NASDAQ_100_Data_From_2010.csv"):
    """Transforms and Loads data into two databricks databases"""

    # Load the data through pandas
    df = pd.read_csv(dataset, nrows=10, sep='\t')
    df.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)  # Rename the column with space
    # print(df)

    # spilt the dataset into two
    # df_price includes the columns Date, Open, High, Low, Close, Adj_Close, Name
    df_price = df[['Date', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Name']]
    # df_volume includes the columns Date, Volume, Name
    df_volume = df[['Date', 'Volume', 'Name']]
    print("Data loaded and splited successfully into two Pandas DataFrames: df_price and df_volume")


    # from .env, load the databricks connection details
    load_dotenv()
    # Connect to the databricks database
    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                    http_path       = os.getenv("HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_KEY")) as connection:
        # SQL commands
        with connection.cursor() as cursor:

            # Create the price table named "hy220_nasdaq_aapl_price"
            create_table_query = """
            CREATE TABLE IF NOT EXISTS hy220_nasdaq_aapl_price (
                Date DATE,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                Close DOUBLE,
                Adj_Close DOUBLE,
                Name STRING
            )
            """
            cursor.execute(create_table_query)

            # Insert the data into the price table
            cursor.execute("SELECT * FROM hy220_nasdaq_aapl_price")  # Check if the table is empty
            if not cursor.fetchall():
                print("Inserting data into the price table...")
                # Convert the DataFrame to SQL statements for multi-row insert
                rows = []
                for index, row in df.iterrows():
                    rows.append(f"('{row['Date']}', {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Adj_Close']}, '{row['Name']}')")
                # Insert the data
                insert_query = f"""
                INSERT INTO hy220_nasdaq_aapl_price (Date, Open, High, Low, Close, Adj_Close, Name) VALUES
                {", ".join(rows)}
                """
                cursor.execute(insert_query)
            else:
                print("Data already exists in the price table")
            

            # Create the volume table named "hy220_nasdaq_aapl_volume"
            create_table_query = """
            CREATE TABLE IF NOT EXISTS hy220_nasdaq_aapl_volume (
                Date DATE,
                Volume DOUBLE,
                Name STRING
            )
            """
            cursor.execute(create_table_query)

            # Insert the data into the volume table
            cursor.execute("SELECT * FROM hy220_nasdaq_aapl_volume")
            if not cursor.fetchall():
                print("Inserting data into the volume table...")
                # Convert the DataFrame to SQL statements for multi-row insert
                rows = []
                for index, row in df.iterrows():
                    rows.append(f"('{row['Date']}', {row['Volume']}, '{row['Name']}')")
                # Insert the data
                insert_query = f"""
                INSERT INTO hy220_nasdaq_aapl_volume (Date, Volume, Name) VALUES
                {", ".join(rows)}
                """
                cursor.execute(insert_query)
            else:
                print("Data already exists in the volume table")

            cursor.close()
        connection.close()

    return "Data loaded or has been loaded successfully"

if __name__ == "__main__":
    load()