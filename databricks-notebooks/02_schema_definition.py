# Databricks notebook source
# MAGIC %md
# MAGIC ### Defining Case classes

# COMMAND ----------

# MAGIC %run "/Users/debavijit.ad@gmail.com/hive_streaming/01_config_and_path"

# COMMAND ----------

# MAGIC %scala
# MAGIC // TotalDistribution and qualityDistribution
# MAGIC case class TrafficStats(
# MAGIC   requests:      Int,
# MAGIC   responses:     Double,
# MAGIC   requestedData: Long,
# MAGIC   receivedData:  Long
# MAGIC )
# MAGIC
# MAGIC // Source traffic vs P2P traffic split
# MAGIC case class TrafficDistribution(
# MAGIC   sourceTraffic: TrafficStats,
# MAGIC   p2pTraffic:    TrafficStats
# MAGIC )
# MAGIC
# MAGIC // Player buffering behaviour
# MAGIC case class PlayerStats(
# MAGIC   bufferings:    Int,
# MAGIC   bufferingTime: Int
# MAGIC )
# MAGIC
# MAGIC // Server-side vs agent-side timestamps
# MAGIC case class TimestampInfo(
# MAGIC   server: Long,
# MAGIC   agent:  Long
# MAGIC )
# MAGIC
# MAGIC //One complete telemetry report from one viewer session
# MAGIC case class TelemetryEvent(
# MAGIC   customerId:          String,
# MAGIC   contentId:           String,
# MAGIC   clientId:            String,
# MAGIC   timestampInfo:       TimestampInfo,
# MAGIC   player:              PlayerStats,
# MAGIC   totalDistribution:   TrafficDistribution,
# MAGIC   qualityDistribution: Map[String, TrafficDistribution]           
# MAGIC )
# MAGIC
# MAGIC println("Case classes defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Derive Schema via Encoders.product

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.Encoders
# MAGIC
# MAGIC val telemetrySchema = Encoders.product[TelemetryEvent].schema
# MAGIC
# MAGIC println("=== Schema derived from TelemetryEvent case class ===")
# MAGIC telemetrySchema.printTreeString()
# MAGIC
# MAGIC println("telemetrySchema ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validate Schema Against Real Data

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{col, explode}
# MAGIC val rawPath = "abfss://raw@streamingdata.dfs.core.windows.net/"
# MAGIC val validationDf = spark.read.schema(telemetrySchema).parquet("abfss://raw@streamingdata.dfs.core.windows.net/eventDate=2025-11-13/")
# MAGIC
# MAGIC println(s"Row count     : ${validationDf.count()}")
# MAGIC println(s"Column count  : ${validationDf.columns.size}")
# MAGIC
# MAGIC // Confirm nested struct reads correctly
# MAGIC println("\n=== Player stats sample ===")
# MAGIC
# MAGIC validationDf.show(5, truncate = false)
# MAGIC
# MAGIC // Confirm map explode works
# MAGIC println("\n=== Quality levels found in data ===")
# MAGIC validationDf
# MAGIC   .select(explode(col("qualityDistribution")).as(Seq("qualityLabel", "traffic")))
# MAGIC   .select("qualityLabel")
# MAGIC   .distinct()
# MAGIC   .orderBy("qualityLabel")
# MAGIC   .show()
# MAGIC
# MAGIC println("Schema validated — safe to proceed to Auto Loader")