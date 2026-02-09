import os
import sys
import asyncio
import logging
import argparse
from dotenv import load_dotenv
from mongo_connect import MongoManager  # Assumes mongo_connect.py is in the same dir

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


async def main():
    """
    Main asynchronous function to run the data export process.
    """

    # --- 1. Setup Command-Line Arguments ---
    parser = argparse.ArgumentParser(description="Export data from MongoDB.")
    parser.add_argument(
        "--sales", action="store_true", help="Set this flag to export sales data."
    )
    parser.add_argument(
        "--weather", action="store_true", help="Set this flag to export weather data."
    )
    args = parser.parse_args()

    if not args.sales and not args.weather:
        log.warning("No export specified. Use --sales or --weather to export data.")
        return

    # --- 2. Load Environment Variables ---
    load_dotenv()
    MONGO_URI = os.environ.get("MONGO_URL")
    DB_NAME = os.environ.get("MONGO_DB")
    SALES_COLLECTION = os.environ.get("MOGNO_SALES_COLLECTION")
    WEATHER_COLLECTION = os.environ.get("MOGNO_WEATHER_COLLECTION")
    OUTPUT_DIR = os.environ.get("DATA_OUTPUT_DIR")

    if not all([MONGO_URI, DB_NAME, OUTPUT_DIR]):
        log.error(
            "Missing environment variables. Ensure MONGO_URL, MONGO_DB, and DATA_OUTPUT_DIR are set."
        )
        return

    if not os.path.exists(OUTPUT_DIR):
        log.info(f"Creating output directory: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR)

    # --- 3. Initialize and Connect Manager ---
    manager = MongoManager(uri=MONGO_URI, db_name=DB_NAME)
    try:
        await manager.connect()

        # --- 4. Export Sales Data (if flagged) ---
        if args.sales:
            sales_filename = os.environ.get("SALES_FILE_NAME")
            if not sales_filename:
                log.error("SALES_FILE_NAME environment variable not set.")
            else:
                log.info(f"Starting Sales data export to {sales_filename}...")
                try:
                    final_path = await manager.pull_data_to_file_in_chunks(
                        collection_name=SALES_COLLECTION,
                        output_dir=OUTPUT_DIR,
                        output_filename=sales_filename,
                        query=None,
                        projection={"_id": 0},
                        chunk_size=500_000,
                    )
                    log.info(f"Sales export successful: {final_path}")
                except Exception as e:
                    log.error(f"An error occurred during sales export: {e}")

        # --- 5. Export Weather Data (if flagged) ---
        if args.weather:
            weather_filename = os.environ.get("WEATHER_FILE_NAME")
            if not weather_filename:
                log.error("WEATHER_FILE_NAME environment variable not set.")
            else:
                log.info(f"Starting Weather data export to {weather_filename}...")
                try:
                    final_path = await manager.pull_data_to_file_in_chunks(
                        collection_name=WEATHER_COLLECTION,
                        output_dir=OUTPUT_DIR,
                        output_filename=weather_filename,
                        query=None,
                        projection={"_id": 0},
                        chunk_size=500_000,
                    )
                    log.info(f"Weather export successful: {final_path}")
                except Exception as e:
                    log.error(f"An error occurred during weather export: {e}")

    except Exception as e:
        log.error(f"An error occurred during the main process: {e}")
    finally:
        log.info("Closing MongoDB connection.")
        await manager.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Script interrupted by user.")


# Code to use to run this script
# python3 1_data_extractor.py --weather
# python3 1_data_extractor.py --sales
# python3 1_data_extractor.py --sales --weather
