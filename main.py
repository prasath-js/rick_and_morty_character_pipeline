import logging
import time

from prefect import flow, task

from pipeline.bronze import ingest_characters_to_bronze
from pipeline.silver import clean_and_flatten_to_silver
from pipeline.gold import prepare_gold_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@task
def _log_start_message(pipeline_name: str) -> None:
    """Logs a starting message for a given pipeline stage."""
    logger.info(f"Starting {pipeline_name} stage...")


@task
def _log_end_message(pipeline_name: str, records_processed: int) -> None:
    """Logs an ending message for a given pipeline stage, including record count."""
    logger.info(f"Finished {pipeline_name} stage. Processed {records_processed} records.")


@flow(name="Rick and Morty Character Pipeline")
def rick_and_morty_character_pipeline() -> None:
    """
    Orchestrates the full Rick and Morty character data pipeline.
    
    This flow sequentially executes the bronze, silver, and gold stages
    of the data pipeline, logging the progress and record counts for each.
    """
    logger.info("🚀 Starting Rick and Morty Character Data Pipeline")
    start_time = time.time()

    # Bronze Layer: Ingest raw data
    _log_start_message("Bronze")
    bronze_records = ingest_characters_to_bronze.submit()
    _log_end_message("Bronze", bronze_records.result())

    # Silver Layer: Clean and flatten data
    _log_start_message("Silver")
    silver_records = clean_and_flatten_to_silver.submit()
    _log_end_message("Silver", silver_records.result())

    # Gold Layer: Prepare final analytical data
    _log_start_message("Gold")
    gold_records = prepare_gold_data.submit()
    _log_end_message("Gold", gold_records.result())

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"✅ Pipeline completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    rick_and_morty_character_pipeline()