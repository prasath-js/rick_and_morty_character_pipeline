import pandas as pd
import psycopg2
from prefect import task, flow
from rick_and_morty_character_pipeline.config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
import pandas.io.sql as sql_io # For potential pandas read_sql errors

@task
def read_silver_data(table_name: str) -> pd.DataFrame:
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
        print(f"Read {len(df)} rows from silver_characters.")
    except (psycopg2.Error, sql_io.DatabaseError) as e: # More specific for database/pandas read_sql errors
        print(f"Error reading silver data from PostgreSQL: {e}")
        raise
    except Exception as e: # Catch any other unexpected errors
        print(f"An unexpected error occurred while reading silver data: {e}")
        raise
    finally:
        if conn:
            conn.close()
    return df

@task
def prepare_gold_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Select only 'id' and 'name' for the gold layer
        gold_df = df[['id', 'name']].copy()
        print(f"Prepared {len(gold_df)} rows for gold layer.")
        return gold_df
    except KeyError as e: # For missing columns
        print(f"Missing key during gold data preparation: {e}")
        raise
    except pd.errors.DataError as e: # For general pandas data handling errors
        print(f"Pandas data error during gold data preparation: {e}")
        raise
    except Exception as e: # Catch any other unexpected errors
        print(f"An unexpected error occurred during gold data preparation: {e}")
        raise

@task
def load_gold_data_to_postgres(df: pd.DataFrame, table_name: str):
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
            name VARCHAR(255)
        );
        """
        cur.execute(create_table_sql)
        conn.commit()

        # Insert data
        for index, row in df.iterrows():
            insert_sql = f"""
            INSERT INTO {table_name} (id, name)
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name;
            """
            cur.execute(insert_sql, (
                row['id'], row['name']
            ))
        conn.commit()
        print(f"Data loaded to {table_name} successfully.")

    except psycopg2.Error as e: # More specific exception for database errors
        print(f"Database error loading gold data to PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e: # Catch any other unexpected errors in load_gold_data_to_postgres
        print(f"An unexpected error occurred during PostgreSQL loading for gold data: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

@flow(name="Rick and Morty Characters Gold Layer")
def gold_layer_pipeline():
    try:
        silver_df = read_silver_data("silver_characters")
        gold_df = prepare_gold_data(silver_df)
        load_gold_data_to_postgres(gold_df, "gold_characters")
    except (psycopg2.Error, sql_io.DatabaseError) as e: # More specific for database/pandas read_sql errors
        print(f"Gold layer pipeline failed due to a database/data reading error: {e}")
        raise
    except pd.errors.PandasError as e: # Catch various pandas related errors during processing
        print(f"Gold layer pipeline failed due to a pandas data processing error: {e}")
        raise
    except Exception as e: # Catch any other unexpected errors in the flow
        print(f"Gold layer pipeline failed due to an unexpected error: {e}")
        raise

if __name__ == "__main__":
    gold_layer_pipeline()
