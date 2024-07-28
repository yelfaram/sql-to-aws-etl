from config import settings
from psycopg2 import connect, sql
from utils.logging import setup_logging

class Database:
    def __init__(self):
        # Configure logging
        self.logger = setup_logging('logs/database.log', 'database')

        # Set up config variables
        self.db_user = settings.DB_USER
        self.db_host = settings.DB_HOST
        self.db_name = settings.DB_NAME
        self.db_password = settings.DB_PASSWORD
        self.db_port = settings.DB_PORT

        # Check if the token/variables were loaded correctly
        if not all([self.db_user, self.db_host, self.db_name, self.db_password, self.db_port]):
            raise ValueError("Database connection details not found in environment variables")

        # Connect to the sql server and then database
        self.conn = self.connect_server()
        self.conn.autocommit = True
        self.create_database()
        self.conn_db = self.connect_db()
        self.conn_db.autocommit = True

    def connect_server(self):
        """Connect to the PostgreSQL server."""
        try:
            conn = connect(
                dbname='postgres',
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
            )
            self.logger.info("PostgreSQL server connected successfully")
            return conn
        except Exception as e:
            self.logger.exception(f"Unexpected error occured while connecting to PostgreSQL server: {e}")
            raise

    def create_database(self):
        """Create the database if it does not exist."""
        try:
            cur = self.conn.cursor()
            cur.execute(sql.SQL('SELECT 1 FROM pg_database WHERE datname = %s'), [self.db_name])
            exists = cur.fetchone()
            if not exists:
                cur.execute(sql.SQL('CREATE DATABASE {};').format(sql.Identifier(self.db_name)))
                self.logger.info(f"Database {self.db_name} created successfully")
            else:
                self.logger.info(f"Database {self.db_name} already exists")
            cur.close()
        except Exception as e:
            self.logger.exception(f"Error occurred while creating database {self.db_name}: {e}")
            raise
    
    def connect_db(self):
        """Connect to the specified database."""
        try:
            conn = connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
            )
            self.logger.info("Database connected successfully")
            return conn
        except Exception as e:
            self.logger.exception(f"Unexpected error occured while connecting to database: {e}")
            raise

    def close_connections(self):
        """Close all database connections."""
        try:
            if self.conn_db:
                self.conn_db.close()
                self.logger.info("Database connection closed")
            if self.conn:
                self.conn.close()
                self.logger.info("PostgreSQL server connection closed")
        except Exception as e:
            self.logger.exception(f"Error occurred while closing connections: {e}")
            raise
