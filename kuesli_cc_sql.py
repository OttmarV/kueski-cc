"""
This PySpark script processes historical stock price data to calculate 7-day moving averages.
It reads stock data from a CSV file, filters for specified stocks (AAPL, AMZN by default),
and calculates moving averages for open, close, adjusted close, low and high price columns.
"""

import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
)

from sql_queries import formatting_filtering_query, moving_average_query

# Avoid inferSchema due of low performance
SCHEMA = StructType(
    [
        StructField("ticker", StringType(), False),
        StructField("open", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("adj_close", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("volume", IntegerType(), False),
        StructField("date", StringType(), False),
    ]
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    """
    Process stock data and calculate 7-day moving average.

    Returns: None
    """
    logger.info("Starting Spark session")
    spark = SparkSession.builder.appName("kueski-cc").getOrCreate()

    logger.info("Loading CSV data")
    try:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(SCHEMA)
            .load("/FileStore/tables/historical_stock_prices.csv")
        )
        logger.info("CSV loaded successfully")
    except Exception as e:
        logger.error("Failed to load CSV: %s", e)
        raise

    df.createOrReplaceTempView("temp_hist_stock")

    logger.info("Filtering data for stocks: %s")
    logger.info("Executing query: %s", formatting_filtering_query)
    df = spark.sql(formatting_filtering_query).repartition(
        2, "ticker"
    )  # Modified as needed

    df.createOrReplaceTempView("temp_moving_average")

    logger.info("Computing 7-day moving averages")
    logger.info("Executing query: %s", moving_average_query)
    df = spark.sql(moving_average_query).cache()

    logger.info("Processing complete")

    # Ordering and showing just for display purposes
    # df = df.orderBy("stock", "date")
    # df.show(10, truncate=False)


if __name__ == "__main__":
    main()
