"""
Main module for the scrape_sentiments project.
This script serves as the entry point for the application.
"""

import sys
import logging
import argparse
import os
from pathlib import Path
import traceback

# Add the project root directory to the Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from scrape_sentiments.scraper import YourStoryScraper, FinshotsScraper
from scrape_sentiments.config import search_terms

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

def parse_arguments():
    """Parse and validate command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Scrape sentiments from websites and analyze them."
    )

    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="The date for which to scrape data (format: YYYY-MM-DD)."
    )

    parser.add_argument(
        "--delta-table-path",
        type=str,
        required=True,
        help="The path to the Delta table where data will be stored."
    )

    parser.add_argument(
        "--website-to-scrape",
        type=str,
        required=True,
        choices=["yourstory", "finshots"],
        help="The website to scrape (choices: 'yourstory', 'finshots')."
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)."
    )

    return parser.parse_args()

def write_to_delta_table(delta_table_path, new_data_arrow, spark):
    """Merge new data into the Delta Lake table using Apache Spark for efficient deduplication, partitioned by website."""

    # Convert new data to Spark DataFrame
    new_data_df = spark.createDataFrame(new_data_arrow.to_pandas())

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        delta_table = DeltaTable.forPath(spark, delta_table_path)

        # Perform merge operation to deduplicate based on unique_key
        delta_table.alias("existing").merge(
            new_data_df.alias("new"),
            "existing.unique_key = new.unique_key"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        # Write new data as a Delta table if it doesn't exist, partitioned by website
        new_data_df.write.format("delta").option("mergeSchema", "true").partitionBy("website").save(delta_table_path)

def vacuum_delta_table(delta_table_path, spark, retention_hours=168):
    """Vacuum the Delta Lake table to remove older versions and free up storage."""

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        delta_table.vacuum(retention_hours)
        logging.info(f"Vacuumed Delta table at {delta_table_path} with retention of {retention_hours} hours.")
    else:
        logging.warning(f"Path {delta_table_path} is not a Delta table. Skipping vacuum.")

def scrape_and_store(date, delta_table_path, website_to_scrape, search_terms):
    """Scrape data from the specified website and store it in a Delta Lake table."""
    spark = SparkSession.builder \
        .appName("DeltaLakeOperations") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .getOrCreate()

    try:
        scraper = None
        if website_to_scrape == "yourstory":
            scraper = YourStoryScraper(date, search_terms)
        elif website_to_scrape == "finshots":
            scraper = FinshotsScraper(date, search_terms)
        if not scraper:
            raise ValueError(f"Unsupported website: {website_to_scrape}")
        df = scraper.scrape()
        if not df.empty:
            df["unique_key"] = df["url"].str.strip().str.lower() + "_$_" + date
            df["scraping_timestamp"] = pd.Timestamp.now()
            df["website"] = website_to_scrape
            new_data_arrow = pa.Table.from_pandas(df)
            write_to_delta_table(delta_table_path, new_data_arrow, spark)
            vacuum_delta_table(delta_table_path, spark)        
    except Exception as e:
        logging.error(f"An error occurred during scraping or storing: {e}")
    finally:
        spark.stop()
        logging.info("WebDriver closed.")

def main():
    """
    Main function to parse arguments, scrape data, and store it.
    """
    args = parse_arguments()

    # Set the logging level based on the argument
    logging.getLogger().setLevel(args.log_level.upper())

    logging.info(f"Starting the scrape_sentiments application for date: {args.date}")
    logging.info(f"Delta table path: {args.delta_table_path}")
    logging.info(f"Website to scrape: {args.website_to_scrape}")

    scrape_and_store(args.date, args.delta_table_path, args.website_to_scrape, search_terms)

    logging.info("Application finished successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.error("Stack trace:")
        logging.error(traceback.format_exc())
        sys.exit(1)