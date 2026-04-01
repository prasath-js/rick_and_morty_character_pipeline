from prefect import flow
from rick_and_morty_character_pipeline.pipeline.bronze import ingest_characters_to_bronze
from rick_and_morty_character_pipeline.pipeline.silver import silver_layer_pipeline
from rick_and_morty_character_pipeline.pipeline.gold import gold_layer_pipeline
import logging

logger = logging.getLogger(__name__)

@flow(name="Full Rick and Morty Data Pipeline")
def full_pipeline_flow():
    """
    Main Prefect flow orchestrating the Bronze, Silver, and Gold data pipelines.
    """
    logger.info("Starting Bronze layer ingestion...")
    bronze_result = ingest_characters_to_bronze()
    logger.info("Bronze layer ingestion completed.")

    logger.info("Starting Silver layer transformation...")
    silver_result = silver_layer_pipeline(wait_for=[bronze_result])
    logger.info("Silver layer transformation completed.")

    logger.info("Starting Gold layer aggregation...")
    gold_result = gold_layer_pipeline(wait_for=[silver_result])
    logger.info("Gold layer aggregation completed.")

    logger.info("Full data pipeline completed successfully!")
