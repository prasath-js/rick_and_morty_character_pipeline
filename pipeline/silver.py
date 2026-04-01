import pandas as pd
import psycopg2
from prefect import task, flow
from psycopg2 import sql
from rick_and_morty_character_pipeline.config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
import pandas.io.sql as sql_io # For potential pandas read_sql errors

@task
def read_bronze_data(table_name: str) -> pd.DataFrame:
    conn = None
    df = pd.DataFrame()
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, conn)
        print(f"Read {len(df)} rows from bronze_characters.")
    except (psycopg2.Error, sql_io.DatabaseError) as e: # More specific for database/pandas read_sql errors
        print(f"Error reading bronze data from PostgreSQL: {e}")
        raise
    except Exception as e: # Catch any other unexpected errors
        print(f"An unexpected error occurred while reading bronze data: {e}")
        raise
    finally:
        if conn:
            conn.close()
    return df

@task
def clean_and_flatten_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Handle empty strings and convert to NaN where appropriate
        df = df.replace(r'^\s*$', pd.NA, regex=True)

        # Flatten nested JSON fields
        df['origin_name'] = df['origin'].apply(lambda x: x.get('name') if isinstance(x, dict) else pd.NA)
        df['location_name'] = df['location'].apply(lambda x: x.get('name') if isinstance(x, dict) else pd.NA)

        # Drop original nested columns
        df = df.drop(columns=['origin', 'location'], errors='ignore')

        # Type casting
        df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
        df['name'] = df['name'].astype(str)
        df['status'] = df['status'].astype(str)
        df['species'] = df['species'].astype(str)
        df['type'] = df['type'].astype(str).replace('<NA>', 'Unknown') # Replace pandas NA string representation
        df['gender'] = df['gender'].astype(str)
        df['image'] = df['image'].astype(str)
        df['url'] = df['url'].astype(str)
        df['created'] = pd.to_datetime(df['created'], errors='coerce')
        df['episode'] = df['episode'].apply(lambda x: [str(item) for item in x] if isinstance(x, list) else []) # Ensure episode elements are strings

        # Ensure no NaN in critical fields after type casting if possible, or handle them
        df['name'] = df['name'].fillna('Unknown')
        df['status'] = df['status'].fillna('Unknown')
        df['species'] = df['species'].fillna('Unknown')
        df['gender'] = df['gender'].fillna('Unknown')
        df['origin_name'] = df['origin_name'].fillna('Unknown')
        df['location_name'] = df['location_name'].fillna('Unknown')

        print(f"Cleaned and flattened {len(df)} rows for silver layer.")
        return df
    except pd.errors.DataError as e: # For general pandas data handling errors
        print(f"Pandas data error during cleaning and flattening: {e}")
        raise
    except KeyError as e: # For missing columns
        print(f"Missing key during cleaning and flattening: {e}")
        raise
    except Exception as e: # Catch any other unexpected errors
        print(f"An unexpected error occurred during cleaning and flattening: {e}")
        raise

@task
def load_silver_data_to_postgres(df: pd.DataFrame, table_name: str):
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()

        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255),
            status VARCHAR(50),
            species VARCHAR(255),
            type VARCHAR(255),
            gender VARCHAR(50),
            origin_name VARCHAR(255),
            location_name VARCHAR(255),
            image VARCHAR(255),
            episode TEXT[],
            url VARCHAR(255),
            created TIMESTAMP
        );
        """
        cur.execute(create_table_sql)
        conn.commit()

        # Insert data
        for index, row in df.iterrows():
            insert_sql = f"""
            INSERT INTO {table_name} (id, name, status, species, type, gender, origin_name, location_name, image, episode, url, created)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                status = EXCLUDED.status,
                species = EXCLUDED.species,
                type = EXCLUDED.type,
                gender = EXCLUDED.gender,
                origin_name = EXCLUDED.origin_name,
                location_name = EXCLUDED.location_name,
                image = EXCLUDED.image,
                episode = EXCLUDED.episode,
                url = EXCLUDED.url,
                created = EXCLUDED.created;
            """
            cur.execute(insert_sql, (
                row['id'], row['name'], row['status'], row['species'], row['type'],
                row['gender'], row['origin_name'], row['location_name'], row['image'],
                row['episode'], row['url'], row['created']
            ))
        conn.commit()
        print(f"Data loaded to {table_name} successfully.")

    except psycopg2.Error as e: # More specific exception for database errors
        print(f"Database error loading silver data to PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e: # Catch any other unexpected errors in load_silver_data_to_postgres
        print(f"An unexpected error occurred during PostgreSQL loading for silver data: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

@flow(name="Rick and Morty Characters Silver Layer")
def silver_layer_pipeline():
    try:
        bronze_df = read_bronze_data("bronze_characters")
        silver_df = clean_and_flatten_to_silver(bronze_df)
        load_silver_data_to_postgres(silver_df, "silver_characters")
    except (psycopg2.Error, sql_io.DatabaseError) as e: # More specific for database/pandas read_sql errors
        print(f"Silver layer pipeline failed due to a database/data reading error: {e}")
        raise
    except pd.errors.PandasError as e: # Catch various pandas related errors during processing
        print(f"Silver layer pipeline failed due to a pandas data processing error: {e}")
        raise
    except Exception as e: # Catch any other unexpected errors in the flow
        print(f"Silver layer pipeline failed due to an unexpected error: {e}")
        raise

if __name__ == "__main__":
    silver_layer_pipeline()
