import pandas as pd
from utils.db_connection import Databaseconfig
from sqlalchemy import inspect
import time

class ETL:
    def __init__(self, data, table_name):
        self.df = data  
        self.table_name = table_name
        self.db_config = Databaseconfig()
        self.engine = self.db_config.get_sql_engine()

    def create_update_tables(self):
        # Automatically create or update tables based on DataFrame schema
        
        # Check if table exists
        inspector = inspect(self.engine)
        if self.table_name in inspector.get_table_names():
            print(f"Table {self.table_name} already exists")
        else:
            print(f"Creating table {self.table_name}")
            self.df.head(0).to_sql(
                self.table_name, 
                con=self.engine, 
                if_exists='replace',  
                index=False
            )
            print(f"Table {self.table_name} created successfully")

    def ingest_data(self, batch_size=1000):
        # Ingest data into the table in batches
        try:
            total_rows = len(self.df)  
            print(f"Starting Ingestion for {total_rows} rows")
            start_time = time.time()
            
            self.df.to_sql(
                self.table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=batch_size
            )
            
            end_time = time.time()
            print(f"{total_rows} rows ingested in {end_time - start_time:.2f} seconds into {self.table_name} table")
            return True

        except Exception as e:
            print("During Ingestion Error Occurred: ", e)
            return False
    
    def run(self, batch_size=1000):
        """Run the complete ETL process"""
        self.create_update_tables()
        return self.ingest_data(batch_size)