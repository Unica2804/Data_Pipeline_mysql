from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, inspect
from sqlalchemy.types import TypeDecorator
import os
import pandas as pd
import urllib.parse
from dotenv import load_dotenv

class Databaseconfig:
    def __init__(self):
        load_dotenv()
        self.db_user=os.getenv("DB_USER", "default_user")
        self.db_password=os.getenv("DB_PASSWORD", "default_password")
        self.host=os.getenv("DB_HOST", "localhost")
        self.port=os.getenv("DB_PORT", "3306")
        self.db_name=os.getenv("DB_NAME", "default_db")
    
    def get_sql_engine(self):
        # Create SQL engine using connection pooling

        password_encoded = urllib.parse.quote_plus(self.db_password) #encoding special characters in password
        connection_string= f"mysql+pymysql://{self.db_user}:{password_encoded}@{self.host}:{self.port}/{self.db_name}"

        engine= create_engine(
            connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=True
        )
        return engine
    
    


