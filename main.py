from rick_and_morty_character_pipeline.orchestration.flows import full_pipeline_flow
import logging
import sys

# Configure basic logging for main.py
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting main pipeline execution...")
    try:
        full_pipeline_flow.serve(name="rick-and-morty-pipeline-deployment",
                                 tags=["rick-and-morty", "data-pipeline"],
                                 description="Deploys the full Rick and Morty character data pipeline.")
    except Exception as e:
        logger.error(f"An error occurred during pipeline deployment or execution: {e}")
    logger.info("Main pipeline execution completed.")
