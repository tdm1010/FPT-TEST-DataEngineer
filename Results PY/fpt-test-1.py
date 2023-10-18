# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

custom_schema = StructType([
    StructField("orig", StringType(), nullable=True),
    StructField("dest", StringType(), nullable=True),
    StructField("id", ShortType(), nullable=True),
    StructField("actl_dep_lcl_tms", TimestampType(), nullable=True),
    StructField("actl_arr_lcl_tms", TimestampType(), nullable=True),
    StructField("flight_num", IntegerType(), nullable=True),
    StructField("flights", IntegerType(), nullable=True),
    StructField("acft_regs_cde", IntegerType(), nullable=True),
    StructField("airborne_lcl_tms", TimestampType(), nullable=True),
    StructField("landing_lcl_tms", TimestampType(), nullable=True),
    StructField("next_flight_id", ShortType(), nullable=True)
])

# COMMAND ----------

# Read data from the Excel file into a DataFrame
csv_file = "/mnt/dataknow/raw/tomas/fpt-test-1.csv"
columns_wanted = ['orig', 'dest', 'id', 'actl_dep_lcl_tms', 'actl_arr_lcl_tms', 'flight_num']

flights_dataset = spark.read.option("header", "true").schema(custom_schema).csv(csv_file).select(columns_wanted)

# Collect the required columns for processing
collect_dataset = flights_dataset.select("id", "dest", "actl_arr_lcl_tms").collect()

# Define the get_next_flight function
def get_next_flight(airport, lcl_tms):
    """
    This function gets the next flight id departure after the arrival fligh to create an array of flights arrival-departure

    :param airport: airport to filter
    :param lcl_tms: timestamp to filter
    :return: next flight id departure on the aircraft
    """ 
    next_flight = flights_dataset.filter(
        (col("orig") == airport) & (col("actl_dep_lcl_tms") > lcl_tms)
    ).orderBy("actl_dep_lcl_tms").first()

    next_flight_id = next_flight["id"] if next_flight else None
    return next_flight_id

# Collect results using a loop
results = []

for row in collect_dataset:
    next_flight_id = get_next_flight(row.dest, row.actl_arr_lcl_tms)
    results.append((row.id, next_flight_id))

result_df = spark.createDataFrame(results, ["id", "next_flight_id"])
final_result = flights_dataset.join(result_df, flights_dataset.id == result_df.id, "inner").drop(result_df.id)
# Display the results
display(final_result.orderBy("orig","actl_dep_lcl_tms"))

# COMMAND ----------

# Writing table
final_result.orderBy("orig","actl_dep_lcl_tms").coalesce(1).write.format("delta").mode("overwrite").save("/mnt/dataknow/raw/tomas/result1")