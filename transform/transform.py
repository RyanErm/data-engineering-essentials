import os
import duckdb

input_file = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

def duckdb_read_parquet(input_file):

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='transform.duckdb', read_only=False)
        # clear out the table if exisits
    con.execute(f"""
        DROP TABLE IF EXISITS yellow_trip_data_202501;
        """)
    print("Table has been dropped")

    con.execute(f"""
        CREATE TABLE yellow_trip_data_202501 AS SELECT * FROM read_parquet('{input_file});
        """)
    print("Records have been sotred in a local folder")

    count = con.execute(f"""
        SELECT COUNT(*) FROM yelloe_trip_data202501;
        """)

    print(f{"Number of records imported imported: {count}")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    duckdb_read_parquet(input_file)
