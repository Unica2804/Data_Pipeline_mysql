from utils.db_connection import Databaseconfig
from utils.Data_Download import KaggleConnect
from src.ETL import ETL
import logging
import pandas as pd
import traceback
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()  # Also print to console
    ]
)

logger = logging.getLogger(__name__)

def main():
    dataset_name="jaderz/hospital-beds-management"
    download_path="./data"
    
    logger.info("="*50)
    logger.info("Starting ETL Pipeline")
    logger.info("="*50)
    
    try:
        # Download dataset
        logger.info(f"Downloading dataset: {dataset_name}")
        kaggle_conn = KaggleConnect(dataset_name, download_path)
        csv_files = kaggle_conn.download_dataset()
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Process each CSV sequentially
        for csv_file in csv_files:
            try:
                logger.info(f"Started processing: {csv_file}")
                
                # Create table name from filename
                file_name = os.path.splitext(os.path.basename(csv_file))[0]
                table_name = file_name.lower().replace(' ', '_')
                
                # Load CSV
                logger.info(f"Loading CSV: {csv_file}")
                df = pd.read_csv(csv_file)
                logger.info(f"Loaded {len(df)} rows from {csv_file}")
                
                # Run ETL
                logger.info(f"Starting ETL for table: {table_name}")
                etl = ETL(data=df, table_name=table_name)
                success = etl.run(batch_size=1000)
                
                if success:
                    logger.info(f"Successfully completed: {table_name} ({len(df)} rows)")
                else:
                    logger.error(f"ETL failed for: {table_name}")
                    
            except Exception as e:
                # Log the full error for this specific file
                logger.error(f"Error processing {csv_file}")
                logger.error(f"Error type: {type(e).__name__}")
                logger.error(f"Error message: {str(e)}")
                logger.error(f"Full traceback:\n{traceback.format_exc()}")
                # Continue with next file
                continue
        
        logger.info("="*50)
        logger.info("ETL Pipeline completed")
        logger.info("="*50)
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")

if __name__ == "__main__":
    main()