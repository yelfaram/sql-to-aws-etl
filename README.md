# COVID-19 Data ETL Project

## Overview

This project fetches COVID-19 case data from a dataset, processes it, and populates it into a PostgreSQL database. It then extracts and transforms the data, saving it into an AWS S3 bucket.

## Project Breakdown

The project was divided into four sprints:

1. **Sprint 1**: Fetch Data from data.world and set up dependencies.
2. **Sprint 2**: Populate Data into PostgreSQL Database.
3. **Sprint 3**: Extract and Perform ETL to AWS S3.
4. **Sprint 4**: Code refactor and documentation.

## Project Structure

- `config/`: Contains configuration settings.
- `data/`: Contains raw and preprocessed CSV files.
- `logs/`: Contains log files generated during the ETL process.
- `utils/`: Contains utility functions used for data cleaning and logging.
- `db/`: Contains scripts related to database operations.
  - `populate_db.py`: Reads CSV file and uploads to PostgreSQL
  - `fetch_data.py`: Fetches dataset from data.world
  - `database.py`: Database class with connections
- `src/`: Contains scripts for the ETL process.
  - `extract_data.py`: Extract logic for the ETL process
  - `transform_data.py`: Transform logic for the ETL process
  - `load_data.py`: Load logic for the ETL process
- `.env`: Contains environment variables for sensitive information.
- `.gitignore`: Git ignore file
- `README.md`: Project readme file
- `requirements.txt`: Lists the Python dependencies for the project
- `main.py`: Main script to run the ETL process

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yelfaram/sql-to-aws-etl.git
    ```
2. Navigate to the project directory:
    ```bash
    cd sql-to-aws-etl
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
    AWS_ACCESS_KEY_ID=your_aws_access_key_id
    AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
    BUCKET_NAME=your_s3_bucket_name
    AWS_REGION=your_aws_region
    ```

## Usage

1. Run the `main.py` script to execute the full ETL process:
    ```bash
    python main.py
    ```

## Logging

Logs are stored in the `logs/` directory and include details of the data fetching, preprocessing, and insertion processes.

