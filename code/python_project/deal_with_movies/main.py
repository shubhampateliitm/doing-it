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
from functools import reduce

# Add the project root directory to the Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from pyspark.sql.functions import col

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

def parse_arguments():
    """Parse and validate command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Scrape sentiments from websites and analyze them."
    )

    parser.add_argument(
        "--task",
        type=str,
        required=True,
        choices=["mean_age", "top_20_highest_rated_movies", "top_genre_rated_by_user", "similar_movies"],
        help="The task to perform (choices: 'mean_age', 'top_20_highest_rated_movies', 'top_genre_rated_by_user', 'similar_movies')."
    )

    parser.add_argument(
        "--delta-table-path",
        type=str,
        required=True,
        help="The path to the Delta table where data will be stored."
    )

    parser.add_argument("--base-data-path", type=str, required=True, help="The base path for the data.")

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)."
    )

    return parser.parse_args()

def write_to_delta_table(delta_table_path, data, spark):
    """Merge new data into the Delta Lake table using Apache Spark for efficient deduplication, partitioned by website."""

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        delta_table = DeltaTable.forPath(spark, delta_table_path)

        # Perform merge operation to deduplicate based on unique_key
        delta_table.alias("existing").merge(
            data.alias("new"),
            "existing.key = new.key"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        # Write new data as a Delta table if it doesn't exist, partitioned by website
        data.write.format("delta").option("mergeSchema", "true").save(delta_table_path)

def vacuum_delta_table(delta_table_path, spark, retention_hours=168):
    """Vacuum the Delta Lake table to remove older versions and free up storage."""

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        delta_table.vacuum(retention_hours)
        logging.info(f"Vacuumed Delta table at {delta_table_path} with retention of {retention_hours} hours.")
    else:
        logging.warning(f"Path {delta_table_path} is not a Delta table. Skipping vacuum.")


def build_spark_session():
    spark = SparkSession.builder \
    .appName("DeltaLakeOperations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()
    return spark

def read_u_data(spark, base_data_path):
    return spark.read.option("delimiter", "\t").option("header", "false").csv(f"{base_data_path}/u.data") \
        .toDF("user_id", "item_id", "rating", "timestamp") \
        .withColumn("rating", col("rating").cast(IntegerType())) \
        .withColumn("timestamp", col("timestamp").cast(IntegerType())) 
    
def read_u_item(spark, base_data_path):
    return spark.read.option("delimiter", "|").option("header", "false").csv(f"{base_data_path}/u.item") \
    .toDF(
    "item_id", "movie_title", "release_date", "video_release_date",
    "IMDb_URL", "unknown", "Action", "Adventure", "Animation",
    "Children's", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
    "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi",
    "Thriller", "War", "Western"
    )

def read_u_genre(spark, base_data_path):
    return spark.read.option("delimiter", "|").option("header", "false").csv(f"{base_data_path}/u.genre") \
    .toDF("genre_name", "genre_id") \
    .withColumn("genre_id", col("genre_id").cast(IntegerType()))

def read_u_user(spark, base_data_path):
    return spark.read.option("delimiter", "|").option("header", "false").csv(f"{base_data_path}/u.user") \
    .toDF("user_id", "age", "gender", "occupation", "zip_code")  


def show_mean_age_by_occupation(u_user):
    """
    Show the mean age of users by occupation.
    """
    mean_age_by_occupation = u_user.groupBy("occupation").agg(mean("age").alias("mean_age")).sort("occupation")
    mean_age_by_occupation.show(100, False)
    return mean_age_by_occupation.withColumn("key", col("occupation"))

def show_top_20_highest_rated_movies(u_data, u_item):
    """
    Show the top 20 highest rated movies.
    """
    top_20_movies = u_data.groupBy("item_id").agg(mean("rating").alias("mean_rating")).sort(col("mean_rating").desc()).limit(20)
    top_20_movies = top_20_movies.join(u_item, "item_id", "inner")
    top_20_movies.show(100, False)
    return top_20_movies.withColumn("key", col("item_id"))

def show_top_genre_rated_by_user(u_data, u_user, u_genre, u_item):
    """
    Show the top genre rated by user.
    """
    genre_list = [row["genre_name"] for row in u_genre.select("genre_name").collect() if row["genre_name"]]
    
    print("was working here")
    from functools import reduce

    genre_with_max_col = reduce(
    lambda acc, c: acc.when(col(c) == col("max_genre_value"), c),
    genre_list[1:],  # start from second genre
    when(col(genre_list[0]) == col("max_genre_value"), genre_list[0])
    )

    print("failing here")
    
    user_grouped = u_user.withColumn("age_group", when(col("age").between(0, 19), "0-19") \
                             .when(col("age").between(20,25), "20-25").when(col("age").between(26, 35), "26-35") \
                             .when(col("age").between(36, 45), "36-45").otherwise("45+"))
    
    fin_data = u_data.join(user_grouped, "user_id").join(u_item, "item_id")
    df = fin_data.withColumn("row_sum",reduce(lambda a, b: a + b, [col(c) for c in genre_list])) \
    .withColumn("norm_rating", col("rating") * 1.0 / col("row_sum")) \
    .groupBy("age_group", "occupation").agg(mean("unknown").alias("unknown"), mean("Action").alias("Action"), 
                                            mean("Adventure").alias("Adventure"), mean("Animation").alias("Animation"), 
                                            mean("Children's").alias("Children's"), mean("Comedy").alias("Comedy"), mean("Crime").alias("Crime"),
                                            mean("Documentary").alias("Documentary"), mean("Drama").alias("Drama"), mean("Fantasy").alias("Fantasy"), 
                                            mean("Film-Noir").alias("Film-Noir"), mean("Horror").alias("Horror"), mean("Musical").alias("Musical"), 
                                            mean("Mystery").alias("Mystery"), mean("Romance").alias("Romance"), mean("Sci-Fi").alias("Sci-Fi"), 
                                            mean("Thriller").alias("Thriller"), mean("War").alias("War"), mean("Western").alias("Western")) \
    .withColumn("max_genre_value", greatest(*[col(c) for c in genre_list])) \
    .withColumn("top_genre", genre_with_max_col) \
    .select("age_group", "occupation", "top_genre", "max_genre_value").sort("age_group", "occupation")

    df.show(100, False)

    return df.withColumn("key", concat(col("age_group"), lit("_"), col("occupation")))



    
def show_similar_movies(u_data, u_item):
    """
    Show similar movies based on user ratings.
    """
    ud2 = u_data.withColumnRenamed("item_id","item_id_2").withColumnRenamed("rating","rating_2")

    result = ud2.join(u_data, "user_id").filter("item_id_2 != item_id").select("user_id", "item_id_2", "item_id", "rating_2", "rating") \
    .withColumn("cross_score", col("rating_2") * col("rating")) \
    .withColumn("r1_2", col("rating") * col("rating")) \
    .withColumn("r2_2", col("rating_2") * col("rating_2")) \
    .groupBy("item_id_2", "item_id") \
    .agg(count("*").alias("cross_count"), sum("cross_score").alias("cross_score"), sum("r1_2").alias("r1_2"), sum("r2_2").alias("r2_2")) \
    .withColumn("cosine", col("cross_score") / (sqrt(col("r1_2")) * sqrt(col("r2_2")))) \
    .filter("cross_count >= 50") \
    .filter("cosine > 0.95") \
    .join(u_item.select(col("item_id"), col("movie_title").alias("movie_title_1")), u_item.item_id == ud2.item_id_2) \
    .drop(u_item.item_id) \
    .join(u_item.select(col("item_id"), col("movie_title").alias("movie_title_2")), "item_id")

    result.show(100, False)
    return result.withColumn("key", col("item_id"))



def main():
    """
    Main function to parse arguments, scrape data, and store it.
    """
    args = parse_arguments()

    # Set the logging level based on the argument
    logging.getLogger().setLevel(args.log_level.upper())

    logging.info(f"Starting the scrape_sentiments application for task: {args.task}")
    logging.info(f"Delta table path: {args.delta_table_path}")

    spark = build_spark_session()
    base_data_path = args.base_data_path
    u_data = read_u_data(spark, base_data_path)
    u_item = read_u_item(spark, base_data_path)
    u_genre = read_u_genre(spark, base_data_path)
    u_user = read_u_user(spark, base_data_path)

    result = None

    if args.task == "mean_age":
        result = show_mean_age_by_occupation(u_user)
    elif args.task == "top_20_highest_rated_movies":
        result = show_top_20_highest_rated_movies(u_data, u_item)
    elif args.task == "top_genre_rated_by_user":
        result = show_top_genre_rated_by_user(u_data, u_user, u_genre, u_item)
    elif args.task == "similar_movies":
        result = show_similar_movies(u_data, u_item)


    if result:
        write_to_delta_table(f"{args.delta_table_path}/{args.task}", result, spark)
        vacuum_delta_table(f"{args.delta_table_path}/{args.task}", spark)
        logging.info(f"Data written to Delta table at {args.delta_table_path}/{args.task}")
    
    logging.info("Application finished successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.error("Stack trace:")
        logging.error(traceback.format_exc())
        sys.exit(1)