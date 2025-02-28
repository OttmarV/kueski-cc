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
from pyspark.sql.window import Window

# Define constants
MV_AVG_DAYS = 7
NUM_DECIMALS = 4
STOCKS = ["AAPL", "AMZN"]  # Add as many valid stocks as needed

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


def calculate_moving_average(df, columns, days=MV_AVG_DAYS, decimals=NUM_DECIMALS):
    """
    Calculate the 7-day moving average for specified columns.

    Args:
        df: DataFrame with stock data
        columns: List of column names to calculate moving averages for.
        days: Default 7, number of days for moving average window.
        decimals: Default 4, number of decimal places for rounding.

    Returns:
        DataFrame with moving average columns added
    """
    logger.info("Starting moving average calculation for columns: %s", columns)
    logger.info("Window size: %s days, Decimal precision: %s", days, decimals)

    window_spec = Window.partitionBy("stock").orderBy(F.col("date"))
    mv_avg_start = (days * -1) + 1

    for column in columns:
        mv_avg_col = f"{column}_moving_avg_{days}_day"
        logger.info(
            "Calculating moving average for column: %s as %s", column, mv_avg_col
        )
        df = df.withColumn(
            mv_avg_col,
            F.when(
                F.row_number().over(window_spec) >= days,
                F.round(
                    F.avg(column).over(window_spec.rowsBetween(mv_avg_start, 0)),
                    decimals,
                ),
            ).otherwise(None),
        )

    logger.info("Moving average calculation completed")
    return df


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

    logger.info("Filtering data for stocks: %s", STOCKS)
    df = (
        df.filter(F.col("ticker").isin(STOCKS))
        # Rename and cast columns. Drop Volume column
        .select(
            F.col("ticker").alias("stock"),
            F.round(F.col("open").cast(DoubleType()), NUM_DECIMALS).alias("open"),
            F.round(F.col("close").cast(DoubleType()), NUM_DECIMALS).alias("close"),
            F.round(F.col("adj_close").cast(DoubleType()), NUM_DECIMALS).alias(
                "adj_close"
            ),
            F.round(F.col("low").cast(DoubleType()), NUM_DECIMALS).alias("low"),
            F.round(F.col("high").cast(DoubleType()), NUM_DECIMALS).alias("high"),
            F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        ).repartition(
            2, "ticker"
        )  # Modified as needed
    )

    logger.info("Computing 7-day moving averages")
    price_columns = ["open", "close", "adj_close", "low", "high"]

    df = calculate_moving_average(df, price_columns).cache()

    logger.info("Processing complete")

    # Ordering and showing just for display purposes
    # df = df.orderBy("stock", "date")
    # df.show(10, truncate=False)


if __name__ == "__main__":
    main()
