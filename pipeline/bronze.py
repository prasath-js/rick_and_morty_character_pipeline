import requests
import pandas as pd
import logging
from prefect import task, flow
from rick_and_morty_character_pipeline.config import RICK_AND_MORTY_API_URL, BRONZE_TABLE_NAME
from rick_and_morty_character_pipeline.utils.db_utils import get_db_connection, create_table, write_dataframe_to_db

logger = logging.getLogger(__name__)

@task
def fetch_characters_from_api(api_url: str):
    """
    Fetches all character data from the Rick and Morty API, handling pagination.
    """
    all_characters = []
    current_url = api_url
    while current_url:
        try:
            response = requests.get(current_url)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            all_characters.extend(data['results'])
            current_url = data['info']['next'] # Get URL for the next page
            logger.info(f"Fetched {len(data['results'])} characters from {response.url}. Next page: {current_url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for URL {current_url}: {e}")
            raise
        except KeyError as e:
            logger.error(f"Unexpected API response structure, missing key: {e}")
            current_url = None # Stop pagination if structure is unexpected
            raise
    logger.info(f"Finished fetching all characters. Total: {len(all_characters)}")
    return all_characters

@task
def load_bronze_data_to_postgres(df: pd.DataFrame, table_name: str):
    """
    Loads the DataFrame to the PostgreSQL bronze table.
    """
    if df.empty:
        logger.warning(f"No data to load to {table_name}. Skipping.")
        return

    conn = None
    try:
        conn = get_db_connection()

        # Define the schema for the bronze_characters table
        bronze_schema = """
id INTEGER PRIMARY KEY,
name VARCHAR(255) NULL,
status VARCHAR(50) NULL,
species VARCHAR(255) NULL,
type VARCHAR(255) NULL,
gender VARCHAR(50) NULL,
origin JSONB NULL,
location JSONB NULL,
image VARCHAR(255) NULL,
episode TEXT[] NULL,
url VARCHAR(255) NULL,
created TIMESTAMP NULL
        """
        create_table(conn, table_name, bronze_schema)

        # Convert 'created' column to datetime objects if it's not already
        df['created'] = pd.to_datetime(df['created'], errors='coerce')

        # List of columns that form the primary key for ON CONFLICT
        conflict_cols = ['id']

        write_dataframe_to_db(conn, df, table_name, conflict_cols)
        logger.info(f"Data loaded to {table_name} successfully.")

    except Exception as e:
        logger.error(f"An error occurred during PostgreSQL loading for bronze data: {e}")
        raise
    finally:
        if conn:
            conn.close()

@flow(name="Ingest Rick and Morty Characters to Bronze")
def ingest_characters_to_bronze():
    """
    Orchestrates the ingestion of Rick and Morty character data into the bronze layer.
    """
    logger.info("Starting bronze layer ingestion flow.")
    try:
        characters = fetch_characters_from_api(RICK_AND_MORTY_API_URL)
        df = pd.DataFrame(characters)
        load_bronze_data_to_postgres(df, BRONZE_TABLE_NAME)
        logger.info("Bronze layer ingestion flow completed successfully.")
        return df # Return df to enable Prefect to pass it downstream
    except Exception as e:
        logger.error(f"Bronze layer ingestion flow failed: {e}")
        raise

if __name__ == "__main__":
    ingest_characters_to_bronze()
