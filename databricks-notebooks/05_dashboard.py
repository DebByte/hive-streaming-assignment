# Databricks notebook source
# MAGIC %run "/Users/debavijit.ad@gmail.com/hive_streaming/01_config_and_path"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Silver Tables

# COMMAND ----------

# MAGIC %scala
# MAGIC val silverBuf  = spark.read.format("delta")
# MAGIC   .load("abfss://silver@streamingdata.dfs.core.windows.net/buffering_metrics/")
# MAGIC val silverQual = spark.read.format("delta")
# MAGIC   .load("abfss://silver@streamingdata.dfs.core.windows.net/quality_metrics/")
# MAGIC
# MAGIC println(s"Buffering rows : ${silverBuf.count()}")
# MAGIC println(s"Quality rows   : ${silverQual.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### QOS overview Chart1

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC println("=== QoS Summary ===")
# MAGIC silverBuf.select(
# MAGIC   count("*").as("totalViewers"),
# MAGIC   sum(when(col("poorQoS") === true, 1).otherwise(0)).as("poorQoSViewers"),
# MAGIC   round(avg("bufferingPct"), 2).as("avgBufferingPct"),
# MAGIC   round(avg("sessionDurationSec"), 0).as("avgSessionSec")
# MAGIC ).show()
# MAGIC
# MAGIC println("=== Buffering Category Distribution ===")
# MAGIC display(
# MAGIC   silverBuf.groupBy("bufferingCategory")
# MAGIC     .count()
# MAGIC     .orderBy("bufferingCategory")
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Buffering Metrices chart2

# COMMAND ----------

# MAGIC %scala
# MAGIC println("=== Top Viewers by Buffering Time ===")
# MAGIC display(
# MAGIC   silverBuf.select(
# MAGIC     col("clientId"),
# MAGIC     col("totalBufferings"),
# MAGIC     col("totalBufferingTimeMs"),
# MAGIC     round(col("bufferingPct"), 2).as("bufferingPct"),
# MAGIC     col("poorQoS"),
# MAGIC     col("bufferingCategory")
# MAGIC   ).orderBy(desc("totalBufferingTimeMs"))
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quality DIstribution Chart3

# COMMAND ----------

# MAGIC %scala
# MAGIC println("=== Quality Consumption by Level ===")
# MAGIC display(
# MAGIC   silverQual.groupBy("qualityLabel", "qualityRank")
# MAGIC     .agg(
# MAGIC       sum("totalSrcReceivedBytes").as("totalSrcBytes"),
# MAGIC       sum("totalP2PReceivedBytes").as("totalP2PBytes"),
# MAGIC       round(avg("p2pRatio"), 3).as("avgP2PRatio"),
# MAGIC       countDistinct("clientId").as("viewerCount")
# MAGIC     )
# MAGIC     .orderBy("qualityRank")
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### P2P vs Source Chart4

# COMMAND ----------

# MAGIC %scala
# MAGIC println("=== P2P vs Source Delivery ===")
# MAGIC display(
# MAGIC   silverQual.groupBy("dominantDelivery")
# MAGIC     .agg(
# MAGIC       countDistinct("clientId").as("viewers"),
# MAGIC       sum("totalReceivedBytes").as("totalBytes")
# MAGIC     )
# MAGIC     .orderBy("dominantDelivery")
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Session duration vs BUffering Chart5

# COMMAND ----------

# MAGIC %scala
# MAGIC println("=== Session Duration vs Buffering % (per viewer) ===")
# MAGIC display(
# MAGIC   silverBuf.select(
# MAGIC     col("clientId"),
# MAGIC     round(col("sessionDurationSec"), 0).as("sessionDurationSec"),
# MAGIC     round(col("bufferingPct"), 2).as("bufferingPct"),
# MAGIC     col("bufferingCategory"),
# MAGIC     col("poorQoS")
# MAGIC   ).orderBy(desc("bufferingPct"))
# MAGIC )