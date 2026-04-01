# Rick and Morty Character Data Pipeline

## Project Overview

This project implements a data pipeline to extract character information from the Rick and Morty API, process it through a Medallion Architecture (Bronze, Silver, Gold layers), and store the results in a PostgreSQL database. The final Gold layer provides a simplified dataset containing only character IDs and names.

### Medallion Architecture

*   **Bronze Layer**: Raw, ingested data directly from the API, preserving its original structure with an added `ingestion_timestamp`.
*   **Silver Layer**: Cleaned, flattened, and type-casted data. Nested fields like `origin` and `location` are flattened, and empty strings are converted to NULLs where appropriate.
*   **Gold Layer**: Transformed data, specifically filtered to contain only the character's `id` and `name` for analytical consumption.

## Stack

*   **Ingestion**: `requests`
*   **Processing**: `Pandas`
*   **Storage**: `PostgreSQL`
*   **Orchestration**: `Prefect`

## Setup Instructions

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/yourusername/rick_and_morty_character_pipeline.git
    cd rick_and_morty_character_pipeline
    ```

2.  **Create a virtual environment and activate it:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: .\venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Set up PostgreSQL:**
    *   Ensure you have a PostgreSQL server running.
    *   Create a database (e.g., `rickandmorty_db`).
    *   **Update `config.py`**: Modify the `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, and `POSTGRES_DB` variables to match your PostgreSQL setup. Alternatively, set these as environment variables.

    Example environment variables:
    ```bash
    export POSTGRES_USER=myuser
    export POSTGRES_PASSWORD=mypassword
    export POSTGRES_HOST=localhost
    export POSTGRES_PORT=5432
    export POSTGRES_DB=rickandmorty_db
    ```

## How to Run the Pipeline

Simply execute the `main.py` file:

```bash
python main.py
```

This will trigger the Prefect flow, which will run the bronze, silver, and gold stages sequentially. You will see log messages in your console indicating the progress and the number of records processed at each stage.

### Expected Output in PostgreSQL

After a successful run, your PostgreSQL database (`rickandmorty_db` or whatever you configured) will contain three tables:

*   `bronze_characters`: Raw JSON data from the API.
*   `silver_characters`: Cleaned, flattened, and typed character data.
*   `gold_character_names`: A table with `id` and `name` for all characters.

## Project Structure

```
rick_and_morty_character_pipeline/
├── data/
│   ├── bronze/           # Placeholder for raw ingested data (Postgres table: bronze_characters)
│   ├── gold/             # Placeholder for final analytics data (Postgres table: gold_character_names)
│   ├── silver/           # Placeholder for cleaned data (Postgres table: silver_characters)
│   └── source/           # Placeholder for external source data
├── pipeline/
│   ├── __init__.py
│   ├── bronze.py         # Raw data ingestion logic
│   ├── gold.py           # Final aggregation and filtering logic
│   └── silver.py         # Data cleaning and transformation logic
├── config.py             # Configuration settings and constants
├── main.py               # Main pipeline runner
├── README.md             # Project documentation
└── requirements.txt      # Python dependencies
```
