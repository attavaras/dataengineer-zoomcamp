# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import pyspark
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()


parser.add_argument('--input_green', required=True) 
parser.add_argument('--input_yellow', required=True) 
parser.add_argument('--output', required=True) 

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

df_green = spark.read.parquet(input_green)

df_green.printSchema()

df_yellow = spark.read.parquet(input_yellow)

df_yellow.printSchema()

df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
                   .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                     .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

df_green.columns

df_yellow.columns

# +
common_columns = []

yellow_columns = df_yellow.columns

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)
# -

common_columns

set(df_green.columns) & set(df_yellow.columns)

df_green.select(common_columns).show()

from pyspark.sql import functions as F

df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
                .select(common_columns) \
                .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.groupBy('service_type').count().show()

df_trips_data.registerTempTable('trips_data')

spark.sql("""
SELECT service_type, count(1) 
FROM 
    trips_data 
GROUP BY 
    service_type 
""").show()

df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    revenue_zone, revenue_month, service_type
""")

df_result.write.parquet(f'{output}/', mode='overwrite')

df_result_read = spark.read.parquet(f'{output}')

df_result_read.registerTempTable('result_read')

spark.sql("""
SELECT COUNT(1) 
FROM 
    result_read
""").show()
