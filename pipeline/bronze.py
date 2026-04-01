import requests
import pandas as pd
import psycopg2
from prefect import task, flow
from rick_and_morty_character_pipeline.config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, RICK_AND_MORTY_API_URL

@task
def fetch_characters_from_api(api_url: str):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()['results']

@task
def load_to_postgres(df: pd.DataFrame, table_name: str):
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
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            status VARCHAR(50),
            species VARCHAR(255),
            type VARCHAR(255),
            gender VARCHAR(50),
            origin JSONB,
            location JSONB,
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
            INSERT INTO {table_name} (id, name, status, species, type, gender, origin, location, image, episode, url, created)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                status = EXCLUDED.status,
                species = EXCLUDED.species,
                type = EXCLUDED.type,
                gender = EXCLUDED.gender,
                origin = EXCLUDED.origin,
                location = EXCLUDED.location,
                image = EXCLUDED.image,
                episode = EXCLUDED.episode,
                url = EXCLUDED.url,
                created = EXCLUDED.created;
            """
            cur.execute(insert_sql, (
                row['id'], row['name'], row['status'], row['species'], row['type'],
                row['gender'], row['origin'], row['location'], row['image'],
                row['episode'], row['url'], row['created']
            ))
        conn.commit()
        print(f"Data loaded to {table_name} successfully.")

    except psycopg2.Error as e: # More specific exception for database errors
        print(f"Database error loading data to PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e: # Catch any other unexpected errors in load_to_postgres
        print(f"An unexpected error occurred during PostgreSQL loading: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cur.close()
            conn.close()


@flow(name="Ingest Rick and Morty Characters to Bronze")
def ingest_characters_to_bronze():
    try:
        characters = fetch_characters_from_api(RICK_AND_MORTY_API_URL)
        df = pd.DataFrame(characters)
        load_to_postgres(df, "bronze_characters")
    except requests.exceptions.RequestException as e: # More specific exception for API calls
        print(f"API request failed: {e}")
        raise
    except pd.errors.EmptyDataError as e: # For potential empty dataframes or parsing issues (e.g., if characters is empty list)
        print(f"Empty data received or DataFrame creation issue: {e}")
        raise
    except psycopg2.Error as e: # More specific exception for database errors within the flow
        print(f"Database error during bronze ingestion flow: {e}")
        raise
    except Exception as e: # Catch any other unexpected errors in ingest_characters_to_bronze
        print(f"An unexpected error occurred during bronze ingestion flow: {e}")
        raise

if __name__ == "__main__":
    ingest_characters_to_bronze()
