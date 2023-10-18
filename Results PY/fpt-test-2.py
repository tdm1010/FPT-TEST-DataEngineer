# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

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

from datetime import datetime, timedelta

# Generate a list of timestamps at 15-minute intervals
timestamps = []
current_time = datetime(2022, 12, 31, 0, 0, 0)
end_time = datetime(2023, 1, 1, 0, 0, 0)
while current_time < end_time:
    timestamps.append(current_time)
    current_time += timedelta(minutes=15)

# Create a DataFrame with TimestampType column
timestamps_df = spark.createDataFrame([(ts,) for ts in timestamps], ["timestamp"])
timestamps_df = timestamps_df.withColumn("timestamp", expr("cast(timestamp as timestamp)"))
# Add a new column with intervals of minus two hours
timestamps_df = timestamps_df.withColumn("two_hour_interval", expr("timestamp - interval 2 hours"))

display(timestamps_df)

# COMMAND ----------

def get_count_in_out_flights(airport, t2, t1):
    """
    This function gets the count of flights within a period of time

    :param airport: airport to filter
    :param t1: start timestamp to filter
    :param t2: end timestamp to filter
    :return: count variables
    """ 
    # Count departure (out) flights
    out_count = flights_dataset.filter(
        (col("orig") == airport) & (col("actl_dep_lcl_tms") > t1) & (col("actl_dep_lcl_tms") <= t2)).count()
    # Count arrival (in) flights
    in_count = flights_dataset.filter(
        (col("dest") == airport) & (col("actl_arr_lcl_tms") > t1) & (col("actl_arr_lcl_tms") <= t2)).count()
    
    return out_count, in_count

# Read data from the Excel file into a DataFrame
csv_file = "/mnt/dataknow/raw/tomas/fpt-test-1.csv"
columns_wanted = ['orig', 'dest', 'id', 'actl_dep_lcl_tms', 'actl_arr_lcl_tms']

flights_dataset = spark.read.option("header", "true").schema(custom_schema).csv(csv_file).select(columns_wanted)

# Create list of timestamp range for every airport
df_list_airports = []

# Iterate over airport lists and call count function
for airport in ['YVR','YYZ']:
    results = []
    collect_dataset = timestamps_df.withColumn("airport", lit(airport)).collect()
    for row in collect_dataset:
        out_count, in_count = get_count_in_out_flights(airport, row.timestamp, row.two_hour_interval)
        results.append((row.airport, row.timestamp, out_count, in_count))

    result_df = spark.createDataFrame(results, ["airport_code", "timestamp", "out", "in"])
    df_list_airports.append(result_df)

final_result = df_list_airports[0].union(df_list_airports[1])

display(final_result)
    

# COMMAND ----------

# Writing table
final_result.coalesce(1).write.format("delta").mode("overwrite").save("/mnt/dataknow/raw/tomas/result2")

# COMMAND ----------

# MAGIC %fs ls /mnt/dataknow/raw/tomas/