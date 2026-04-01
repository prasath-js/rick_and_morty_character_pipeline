import pandas as pd
import logging
from prefect import task, flow
from rick_and_morty_character_pipeline.config import SILVER_TABLE_NAME, GOLD_TABLE_NAME
from rick_and_morty_character_pipeline.utils.db_utils import get_db_connection, create_table, write_dataframe_to_db, read_data_from_db

logger = logging.getLogger(__name__)

@task
def read_silver_data_gold_task(table_name: str) -> pd.DataFrame:
    """Reads silver data from PostgreSQL."""
    logger.info(f"Attempting to read data from silver table: {table_name}")
    try:
        df = read_data_from_db(table_name)
        logger.info(f"Successfully read {len(df)} rows from '{table_name}'.")
        return df
    except Exception as e:
        logger.error(f"Error reading silver data from PostgreSQL for gold layer: {e}")
        raise

@task
def prepare_gold_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares the gold layer DataFrame by selecting 'id' and 'name'.
    """
    logger.info(f"Starting gold data preparation for {len(df)} rows.")
    if df.empty:
        logger.warning("Input DataFrame for gold preparation is empty.")
        return pd.DataFrame()

    try:
        # Select only 'id' and 'name' for the gold layer
        gold_df = df[['id', 'name']].copy()
        logger.info(f"Prepared {len(gold_df)} rows for gold layer.")
        return gold_df
    except KeyError as e:
        logger.error(f"Missing key during gold data preparation: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during gold data preparation: {e}")
        raise

@task
def load_gold_data_to_postgres(df: pd.DataFrame, table_name: str):
    """
    Loads the prepared gold DataFrame to the PostgreSQL gold table.
    """
    if df.empty:
        logger.warning(f"No data to load to {table_name}. Skipping.")
        return

    conn = None
    try:
        conn = get_db_connection()

        # Define the schema for the gold_character_names table
        gold_schema = """
id BIGINT PRIMARY KEY,
name VARCHAR(255) NULL
        """
        create_table(conn, table_name, gold_schema)

        # List of columns that form the primary key for ON CONFLICT
        conflict_cols = ['id']

        write_dataframe_to_db(conn, df, table_name, conflict_cols)
        logger.info(f"Data loaded to {table_name} successfully.")

    except Exception as e:
        logger.error(f"An error occurred during PostgreSQL loading for gold data: {e}")
        raise
    finally:
        if conn:
            conn.close()

@flow(name="Rick and Morty Characters Gold Layer")
def gold_layer_pipeline():
    """
    Orchestrates the gold layer data processing.
    """
    logger.info("Starting gold layer pipeline.")
    try:
        silver_df = read_silver_data_gold_task(SILVER_TABLE_NAME)
        gold_df = prepare_gold_data(silver_df)
        load_gold_data_to_postgres(gold_df, GOLD_TABLE_NAME)
        logger.info("Gold layer pipeline completed successfully.")
        return gold_df # Return df to enable Prefect to pass it downstream
    except Exception as e:
        logger.error(f"Gold layer pipeline failed: {e}")
        raise

if __name__ == "__main__":
    gold_layer_pipeline()
