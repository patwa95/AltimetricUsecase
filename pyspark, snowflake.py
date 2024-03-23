# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


customerTxnDf = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/patwavikash95@gmail.com/CustomerTransaction.csv")
summary_stats = customerTxnDf.selectExpr(
    "count(*) as Count",
    "mean(`Transaction Amount`) as Mean",
    "percentile_approx(`Transaction Amount`, 0.5) as Median",
    "stddev_samp(`Transaction Amount`) as StdDev"
)


summary_stats.write.format("snowflake") \
    .option("sfURL", "your_snowflake_url") \
    .option("sfUser", "your_snowflake_user") \
    .option("sfPassword", "your_snowflake_password") \
    .option("sfDatabase", "your_snowflake_database") \
    .option("sfSchema", "your_snowflake_schema") \
    .option("sfWarehouse", "your_snowflake_warehouse") \
    .option("dbtable", "your_snowflake_stage_table") \
    .mode("overwrite") 
    .save()




spark.stop()


# COMMAND ----------


