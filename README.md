# COVID-19 Data ETL Project

## Overview

This project fetches COVID-19 case data from a dataset, processes it, and populates it into a PostgreSQL database. It then extracts and transforms the data, saving it into an AWS S3 bucket.

## Project Structure

- `data/`: Contains raw and preprocessed CSV files.
- `logs/`: Contains log files generated during the ETL process.
- `connection.py`: Handles database connections.
- `dbconfig.py`: Stores configuration variables for database connections.
- `fetch_data.py`: Fetches data from Data World and preprocesses it.
- `populate_db.py`: Processes CSV data and inserts it into the PostgreSQL database.
- `requirements.txt`: Lists the Python dependencies for the project.
- `.env`: Contains environment variables for sensitive information.

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yelfaram/sql-to-aws-etl.git
    ```
2. Navigate to the project directory:
    ```bash
    cd your_project_directory
    ```
3. Create a virtual environment and activate it:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
4. Install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

1. Create a `.env` file in the project root with the following content:
    ```
    DW_AUTH_TOKEN=your_data_world_token
    DATASET_KEY="zathompson/covid-19-case-counts-test"
    DATASET_TABLE_NAME="covid_19_cases"
    DB_USER=your_db_user
    DB_HOST=your_db_host
    DB_NAME=your_db_name
    DB_PASSWORD=your_db_password
    DB_PORT=your_db_port
    ```

## Usage

1. Run the `fetch_data.py` script to fetch and preprocess data:
    ```bash
    python .\db\fetch_data.py
    ```
2. Run the `populate_db.py` script to create the table and insert data into PostgreSQL:
    ```bash
    python .\db\populate_db.py
    ```

## Logging

Logs are stored in the `logs/` directory and include details of the data fetching, preprocessing, and insertion processes.

