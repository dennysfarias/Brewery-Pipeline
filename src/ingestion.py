import os
import json
import logging
from api_client import OpenBreweryClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_to_bronze(bronze_path):
    """
    Fetches data from the API and saves it to the Bronze layer.
    """
    logger.info("Starting ingestion to Bronze layer...")
    
    client = OpenBreweryClient()
    try:
        data = client.fetch_all_breweries()
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        raise

    # Ensure directory exists
    os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
    
    logger.info(f"Saving {len(data)} records to {bronze_path}")
    
    try:
        with open(bronze_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info("Ingestion complete.")
    except Exception as e:
        logger.error(f"Failed to write Bronze file: {e}")
        raise
