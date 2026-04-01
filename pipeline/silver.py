import pandas as pd
import logging
from prefect import task, flow
from rick_and_morty_character_pipeline.config import BRONZE_TABLE_NAME, SILVER_TABLE_NAME
from rick_and_morty_character_pipeline.utils.db_utils import get_db_connection, create_table, write_dataframe_to_db, read_data_from_db

logger = logging.getLogger(__name__)

@task
def read_bronze_data_silver_task(table_name: str) -> pd.DataFrame:
    """Reads bronze data from PostgreSQL."""
    logger.info(f"Attempting to read data from bronze table: {table_name}")
    try:
        df = read_data_from_db(table_name)
        logger.info(f"Successfully read {len(df)} rows from '{table_name}'.")
        return df
    except Exception as e:
        logger.error(f"Error reading bronze data from PostgreSQL for silver layer: {e}")
        raise

@task
def clean_and_flatten_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans, flattens, and transforms bronze data into a silver layer DataFrame.
    Handles 'origin.url' and 'location.url' and converts pd.NA to None for nullable fields.
    """
    logger.info(f"Starting cleaning and flattening for {len(df)} rows.")
    if df.empty:
        logger.warning("Input DataFrame for cleaning is empty.")
        return pd.DataFrame()

    try:
        # Handle empty strings and convert to NaN where appropriate for object columns
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].replace(r'^\s*$', pd.NA, regex=True)

        # Flatten nested JSON fields and extract URLs
        df['origin_name'] = df['origin'].apply(lambda x: x.get('name') if isinstance(x, dict) else pd.NA)
        df['origin_url'] = df['origin'].apply(lambda x: x.get('url') if isinstance(x, dict) else pd.NA)
        df['location_name'] = df['location'].apply(lambda x: x.get('name') if isinstance(x, dict) else pd.NA)
        df['location_url'] = df['location'].apply(lambda x: x.get('url') if isinstance(x, dict) else pd.NA)

        # Drop original nested columns
        df = df.drop(columns=['origin', 'location'], errors='ignore')

        # Type casting and conversion of pd.NA to None for database compatibility
        df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
        df['name'] = df['name'].astype(str).replace({pd.NA: None, 'nan': None}) # .replace('nan', None) for string 'nan'
        df['status'] = df['status'].astype(str).replace({pd.NA: None, 'nan': None})
        df['species'] = df['species'].astype(str).replace({pd.NA: None, 'nan': None})
        df['type'] = df['type'].astype(str).replace({pd.NA: None, 'nan': None}) # Convert to None instead of 'Unknown'
        df['gender'] = df['gender'].astype(str).replace({pd.NA: None, 'nan': None})
        df['image'] = df['image'].astype(str).replace({pd.NA: None, 'nan': None})
        df['url'] = df['url'].astype(str).replace({pd.NA: None, 'nan': None})
        df['created'] = pd.to_datetime(df['created'], errors='coerce') # NaT will be converted to None by db_utils
        df['episode'] = df['episode'].apply(lambda x: [str(item) for item in x] if isinstance(x, list) else None)

        df['origin_name'] = df['origin_name'].astype(str).replace({pd.NA: None, 'nan': None})
        df['origin_url'] = df['origin_url'].astype(str).replace({pd.NA: None, 'nan': None})
        df['location_name'] = df['location_name'].astype(str).replace({pd.NA: None, 'nan': None})
        df['location_url'] = df['location_url'].astype(str).replace({pd.NA: None, 'nan': None})

        logger.info(f"Cleaned and flattened {len(df)} rows for silver layer.")
        return df
    except Exception as e:
        logger.error(f"An error occurred during cleaning and flattening for silver layer: {e}")
        raise

@task
def load_silver_data_to_postgres(df: pd.DataFrame, table_name: str):
    """
    Loads the cleaned and flattened DataFrame to the PostgreSQL silver table.
    """
    if df.empty:
        logger.warning(f"No data to load to {table_name}. Skipping.")
        return

    conn = None
    try:
        conn = get_db_connection()

        # Define the schema for the silver_characters table including new URL fields and NULLable columns
        silver_schema = """
id BIGINT PRIMARY KEY,
name VARCHAR(255) NULL,
status VARCHAR(50) NULL,
species VARCHAR(255) NULL,
type VARCHAR(255) NULL,
gender VARCHAR(50) NULL,
origin_name VARCHAR(255) NULL,
origin_url VARCHAR(255) NULL,
location_name VARCHAR(255) NULL,
location_url VARCHAR(255) NULL,
image VARCHAR(255) NULL,
episode TEXT[] NULL,
url VARCHAR(255) NULL,
created TIMESTAMP NULL
        """
        create_table(conn, table_name, silver_schema)

        # List of columns that form the primary key for ON CONFLICT
        conflict_cols = ['id']

        write_dataframe_to_db(conn, df, table_name, conflict_cols)
        logger.info(f"Data loaded to {table_name} successfully.")

    except Exception as e:
        logger.error(f"An error occurred during PostgreSQL loading for silver data: {e}")
        raise
    finally:
        if conn:
            conn.close()

@flow(name="Rick and Morty Characters Silver Layer")
def silver_layer_pipeline():
    """
    Orchestrates the silver layer data processing.
    """
    logger.info("Starting silver layer pipeline.")
    try:
        bronze_df = read_bronze_data_silver_task(BRONZE_TABLE_NAME)
        silver_df = clean_and_flatten_to_silver(bronze_df)
        load_silver_data_to_postgres(silver_df, SILVER_TABLE_NAME)
        logger.info("Silver layer pipeline completed successfully.")
        return silver_df # Return df to enable Prefect to pass it downstream
    except Exception as e:
        logger.error(f"Silver layer pipeline failed: {e}")
        raise

if __name__ == "__main__":
    silver_layer_pipeline()
