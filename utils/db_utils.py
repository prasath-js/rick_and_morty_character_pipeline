import psycopg2
import pandas as pd
from psycopg2 import sql, extras
from rick_and_morty_character_pipeline.config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
import logging

logger = logging.getLogger(__name__)

def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        logger.info("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

def create_table(conn, table_name: str, schema: str):
    """
    Creates a table if it doesn't already exist.
    Args:
        conn: The database connection object.
        table_name: The name of the table to create.
        schema: The SQL schema definition for the table columns.
    """
    cur = None
    try:
        cur = conn.cursor()
        create_table_sql = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
        ).format(
            table_name=sql.Identifier(table_name),
            schema=sql.SQL(schema)
        )
        cur.execute(create_table_sql)
        conn.commit()
        logger.info(f"Table '{table_name}' checked/created successfully.")
    except psycopg2.Error as e:
        logger.error(f"Error creating table '{table_name}': {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during table creation '{table_name}': {e}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()

def write_dataframe_to_db(conn, df: pd.DataFrame, table_name: str, conflict_cols: list):
    """
    Writes a pandas DataFrame to a PostgreSQL table, handling conflicts by updating.
    Uses psycopg2.extras.execute_values for efficient bulk inserts/updates.
    Args:
        conn: The database connection object.
        df: The pandas DataFrame to write.
        table_name: The name of the target table.
        conflict_cols: A list of column names to use in the ON CONFLICT clause (e.g., ['id']).
    """
    if df.empty:
        logger.info(f"DataFrame for table '{table_name}' is empty. Skipping write operation.")
        return

    cur = None
    try:
        cur = conn.cursor()

        # Convert DataFrame to list of tuples, replacing pd.NA with None
        data_to_insert = [
            tuple(None if pd.isna(item) else item for item in row)
            for row in df.itertuples(index=False)
        ]

        columns = df.columns.tolist()
        columns_sql = sql.SQL(', ').join(map(sql.Identifier, columns))

        conflict_target = sql.SQL(', ').join(map(sql.Identifier, conflict_cols))
        update_set = sql.SQL(', ').join(
            sql.SQL('{0} = EXCLUDED.{0}').format(sql.Identifier(col))
            for col in columns if col not in conflict_cols
        )

        # Dynamically create the placeholders string based on number of columns
        # For execute_values, values are provided as a list of tuples, and %s is used once for the whole set
        insert_sql = sql.SQL(
            "INSERT INTO {table_name} ({columns}) VALUES %s "
            "ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set}"
        ).format(
            table_name=sql.Identifier(table_name),
            columns=columns_sql,
            conflict_target=conflict_target,
            update_set=update_set
        )

        extras.execute_values(cur, insert_sql, data_to_insert)
        conn.commit()
        logger.info(f"Successfully wrote {len(df)} rows to '{table_name}'.")

    except psycopg2.Error as e:
        logger.error(f"Error writing DataFrame to table '{table_name}': {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while writing DataFrame to table '{table_name}': {e}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()

def read_data_from_db(table_name: str) -> pd.DataFrame:
    """Reads data from a specified table into a pandas DataFrame."""
    conn = None
    try:
        conn = get_db_connection()
        query = sql.SQL("SELECT * FROM {table_name}").format(table_name=sql.Identifier(table_name))
        df = pd.read_sql(query.as_string(conn), conn)
        logger.info(f"Read {len(df)} rows from '{table_name}'.")
        return df
    except Exception as e:
        logger.error(f"Error reading data from table '{table_name}': {e}")
        raise
    finally:
        if conn:
            conn.close()
