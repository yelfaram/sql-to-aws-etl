import os
from dotenv import load_dotenv
load_dotenv()

# Set up config variables
db_user = os.getenv('DB_USER')
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
db_password = os.getenv('DB_PASSWORD')
db_port = os.getenv('DB_PORT')

# Check if the token/variables were loaded correctly
if not all([db_user, db_host, db_name, db_password,db_port]):
    raise ValueError("Database connection details not found in environment variables")