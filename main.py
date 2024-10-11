import lib

# Define file paths
db_path = 'data/nasdaq.db'
csv_path = 'data/NASDAQ_100_Data_From_2010.csv'

# Load data
lib.my_load(db_path=db_path, csv_path=csv_path, nrows=3)
print("Data loaded into the database.")

# Create a new record
new_record = ('2021-10-01', 100, 110, 90, 105, 1000000, 'Imaginary AAPL')
lib.my_create(db_path=db_path, record=new_record)
print("New record created.")

# Read the records
results = lib.my_read(db_path=db_path, name='Imaginary AAPL')
print("Read records:")
for row in results:
    print(row)

# Update the record
lib.my_update(db_path=db_path, name='Imaginary AAPL', new_close=200)
print("Record updated.")

# Read the records after update
results = lib.my_read(db_path=db_path, name='Imaginary AAPL')
print("Read records after update:")
for row in results:
    print(row)

# Delete the record
lib.my_delete(db_path=db_path, name='Imaginary AAPL')
print("Record deleted.")

# Read the records after deletion
results = lib.my_read(db_path=db_path, name='Imaginary AAPL')
print("Read records after deletion:")
print(results)
