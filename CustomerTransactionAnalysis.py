# Databricks notebook source
customerTxnDf = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/patwavikash95@gmail.com/CustomerTransaction.csv")

# COMMAND ----------

display(customerTxnDf)

# COMMAND ----------

# DBTITLE 1,Mean ,median ,stdDev calculation
summary_stats = customerTxnDf.selectExpr(
    "count(*) as Count",
    "mean(`Transaction Amount`) as Mean",
    "percentile_approx(`Transaction Amount`, 0.5) as Median",
    "stddev_samp(`Transaction Amount`) as StdDev"
)
display(summary_stats)

# COMMAND ----------


