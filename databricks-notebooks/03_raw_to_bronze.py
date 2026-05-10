# Databricks notebook source
# MAGIC %md
# MAGIC ###01_config_and_path

# COMMAND ----------

# MAGIC %run "/Users/debavijit.ad@gmail.com/hive_streaming/01_config_and_path"

# COMMAND ----------

# MAGIC %md
# MAGIC ###02_schema_definition

# COMMAND ----------

# MAGIC %run "/Users/debavijit.ad@gmail.com/hive_streaming/02_schema_definition"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Imports

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.sql.functions.{current_timestamp, current_date, input_file_name, lit, col, regexp_extract, to_date}
# MAGIC import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
# MAGIC import io.delta.tables.DeltaTable
# MAGIC import java.time.LocalDate
# MAGIC
# MAGIC println("Imports ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingestion Function

# COMMAND ----------

# DBTITLE 1,Ingestion Function
# MAGIC %scala
# MAGIC def ingestRawToBronze(): StreamingQuery = {
# MAGIC     val ingestedDate = LocalDate.now().toString
# MAGIC     val jsonStream = spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.schemaLocation", "abfss://bronze@streamingdata.dfs.core.windows.net/_json_schema/")
# MAGIC         .option("cloudFiles.inferColumnTypes", "true")
# MAGIC         .load(Paths.raw)
# MAGIC         .withColumn("eventDate",to_date(regexp_extract(col("_metadata.file_path"),"eventDate=([0-9]{4}-[0-9]{2}-[0-9]{2})",1)))
# MAGIC         .withColumn("_ingested_at", current_timestamp())
# MAGIC         .withColumn("_source_file", col("_metadata.file_path"))
# MAGIC         .withColumn("_ingested_date", lit(ingestedDate))
# MAGIC     
# MAGIC     rawStream.writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation", Paths.autoloaderCheckpoint)
# MAGIC         .outputMode("append")
# MAGIC         .option("mergeSchema", "true")
# MAGIC         .partitionBy("eventDate", "_ingested_date")
# MAGIC         .trigger(Trigger.AvailableNow())
# MAGIC         .start(Paths.bronzeTelemetry)
# MAGIC }
# MAGIC
# MAGIC println("ingestRawToBronze defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute

# COMMAND ----------

# MAGIC %scala
# MAGIC println(s"Source      : ${Paths.raw}")
# MAGIC println(s"Destination : ${Paths.bronzeTelemetry}")
# MAGIC println(s"Schema store: ${Paths.autoloaderSchema}")
# MAGIC println(s"Checkpoint  : ${Paths.autoloaderCheckpoint}")
# MAGIC println("Starting ingestion...")
# MAGIC
# MAGIC val bronzeQuery = ingestRawToBronze()
# MAGIC val completedCleanly = bronzeQuery.awaitTermination(timeoutMs = 10 * 60 * 1000L)
# MAGIC if (completedCleanly)
# MAGIC   println("Ingestion complete — stopped automatically")
# MAGIC else {
# MAGIC   println("Timeout reached — stopping manually")
# MAGIC   bronzeQuery.stop()
# MAGIC }
# MAGIC
# MAGIC println("Raw to Bronze ingestion complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Bronze

# COMMAND ----------

# MAGIC %scala
# MAGIC val bronzeDf = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(Paths.bronzeTelemetry)
# MAGIC
# MAGIC println(s"Total rows ingested : ${bronzeDf.count()}")
# MAGIC println(s"Total columns       : ${bronzeDf.columns.size}")
# MAGIC
# MAGIC println("\n=== Lineage column null check ===")
# MAGIC import org.apache.spark.sql.functions.{col, count, when}
# MAGIC bronzeDf.select(
# MAGIC   count(when(col("_ingested_at").isNull, 1)).as("null_ingested_at"),
# MAGIC   count(when(col("_source_file").isNull, 1)).as("null_source_file"),
# MAGIC   count(when(col("_ingested_date").isNull, 1)).as("null_ingested_date")
# MAGIC ).show()
# MAGIC
# MAGIC println("\n=== Bronze schema (including metadata cols) ===")
# MAGIC bronzeDf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Idempotency Check

# COMMAND ----------

# MAGIC %scala
# MAGIC val countBefore = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(Paths.bronzeTelemetry)
# MAGIC   .count()
# MAGIC
# MAGIC println(s"Row count BEFORE rerun : $countBefore")
# MAGIC
# MAGIC val rerunQuery = ingestRawToBronze()
# MAGIC rerunQuery.awaitTermination()
# MAGIC
# MAGIC val countAfter = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(Paths.bronzeTelemetry)
# MAGIC   .count()
# MAGIC
# MAGIC println(s"Row count AFTER rerun  : $countAfter")
# MAGIC
# MAGIC if (countBefore == countAfter)
# MAGIC   println("no duplicates on rerun")
# MAGIC else
# MAGIC   println(s"Count changed by ${countAfter - countBefore} rows")