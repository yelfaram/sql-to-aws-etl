import subprocess
import os
from db import fetch_data
from db import populate_db
from src import extract_data
from src import transform_data
from src import load_data

def main():
    # Ensure the current working directory is set correctly
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    print("Fetching data...")
    fetch_data.main()

    print("Connecting to database and populating...")
    populate_db.main()

    print("Extracting data...")
    extract_data.main()

    print("Transforming data...")
    transform_data.main()

    print("Loading data...")
    load_data.main()

    print("ETL process complete.")

if __name__ == "__main__":
    main()