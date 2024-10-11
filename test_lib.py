import lib

def test_my_load():
    # Define file paths
    db_path = 'data/test_nasdaq.db'
    csv_path = 'data/NASDAQ_100_Data_From_2010.csv'

    # Load data
    lib.my_load(db_path=db_path, csv_path=csv_path, nrows=3)
    # Check if data was loaded
    results = lib.my_read(db_path=db_path, name='AAPL')
    assert len(results) > 0
    print("test_my_load passed.")

def test_my_create():
    db_path = 'data/test_nasdaq.db'
    # Insert a new record
    test_record = ('2021-10-01', 100, 110, 90, 105, 1000000, 'Test Stock')
    lib.my_create(db_path=db_path, record=test_record)
    # Check if the record was inserted
    results = lib.my_read(db_path=db_path, name='Test Stock')
    assert len(results) == 1
    print("test_my_create passed.")

def test_my_read():
    db_path = 'data/test_nasdaq.db'
    # Read the record
    results = lib.my_read(db_path=db_path, name='Test Stock')
    assert len(results) == 1
    print("test_my_read passed.")

def test_my_update():
    db_path = 'data/test_nasdaq.db'
    # Update the record
    lib.my_update(db_path=db_path, name='Test Stock', new_close=200)
    # Check if the record was updated
    results = lib.my_read(db_path=db_path, name='Test Stock')
    assert results[0][4] == 200  # 'Close' is at index 4
    print("test_my_update passed.")

def test_my_delete():
    db_path = 'data/test_nasdaq.db'
    # Delete the record
    lib.my_delete(db_path=db_path, name='Test Stock')
    # Check if the record was deleted
    results = lib.my_read(db_path=db_path, name='Test Stock')
    assert len(results) == 0
    print("test_my_delete passed.")

if __name__ == "__main__":
    test_my_load()
    test_my_create()
    test_my_read()
    test_my_update()
    test_my_delete()
    print("All tests passed.")
