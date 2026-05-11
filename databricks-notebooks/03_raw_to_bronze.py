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

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{current_timestamp, lit, col, regexp_extract, to_date}
# MAGIC import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
# MAGIC import java.time.LocalDate
# MAGIC
# MAGIC def ingestParquetStream(): StreamingQuery = {
# MAGIC     val ingestedDate = LocalDate.now().toString
# MAGIC     spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "parquet")
# MAGIC         .option("pathGlobFilter", "*.parquet")           
# MAGIC         .option("cloudFiles.schemaLocation", Paths.autoloaderSchema)
# MAGIC         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC         .option("cloudFiles.includeExistingFiles", "true")
# MAGIC         .load(Paths.raw)
# MAGIC         .withColumn("eventDate", to_date(regexp_extract(
# MAGIC             col("_metadata.file_path"),
# MAGIC             "eventDate=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1)))
# MAGIC         .withColumn("_ingested_at",   current_timestamp())
# MAGIC         .withColumn("_source_file",   col("_metadata.file_path"))
# MAGIC         .withColumn("_ingested_date", lit(ingestedDate))
# MAGIC         .writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation",
# MAGIC             "abfss://bronze@streamingdata.dfs.core.windows.net/_checkpoint_parquet/")
# MAGIC         .outputMode("append")
# MAGIC         .option("mergeSchema", "true")
# MAGIC         .partitionBy("eventDate", "_ingested_date")
# MAGIC         .trigger(Trigger.AvailableNow())
# MAGIC         .start(Paths.bronzeTelemetry)
# MAGIC }
# MAGIC
# MAGIC def ingestJsonStream(): StreamingQuery = {
# MAGIC     val ingestedDate = LocalDate.now().toString
# MAGIC    // Explicit schema — no inference
# MAGIC     import org.apache.spark.sql.types._
# MAGIC
# MAGIC     val trafficStatsSchema = StructType(Seq(
# MAGIC         StructField("requests",      IntegerType),
# MAGIC         StructField("responses",     DoubleType),
# MAGIC         StructField("requestedData", LongType),
# MAGIC         StructField("receivedData",  LongType)
# MAGIC     ))
# MAGIC     val trafficDistSchema = StructType(Seq(
# MAGIC         StructField("sourceTraffic", trafficStatsSchema),
# MAGIC         StructField("p2pTraffic",    trafficStatsSchema)
# MAGIC     ))
# MAGIC     val jsonSchema = StructType(Seq(
# MAGIC         StructField("customerId",          StringType),
# MAGIC         StructField("contentId",           StringType),
# MAGIC         StructField("clientId",            StringType),
# MAGIC         StructField("timestampInfo",       StructType(Seq(
# MAGIC             StructField("server", LongType),
# MAGIC             StructField("agent",  LongType)
# MAGIC         ))),
# MAGIC         StructField("player",              StructType(Seq(
# MAGIC             StructField("bufferings",    IntegerType),
# MAGIC             StructField("bufferingTime", IntegerType)
# MAGIC         ))),
# MAGIC         StructField("totalDistribution",   trafficDistSchema),
# MAGIC         StructField("qualityDistribution", MapType(StringType, trafficDistSchema))
# MAGIC     ))
# MAGIC
# MAGIC     spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("pathGlobFilter", "*.json") 
# MAGIC         .option("cloudFiles.schemaLocation",
# MAGIC             "abfss://bronze@streamingdata.dfs.core.windows.net/_checkpoint_json_schema/")
# MAGIC         .option("cloudFiles.includeExistingFiles", "true")
# MAGIC         .schema(jsonSchema)  
# MAGIC         .option("cloudFiles.includeExistingFiles", "true")
# MAGIC         .load(Paths.raw)
# MAGIC         .withColumn("eventDate", to_date(regexp_extract(
# MAGIC             col("_metadata.file_path"),
# MAGIC             "eventDate=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1)))
# MAGIC         .withColumn("_ingested_at",   current_timestamp())
# MAGIC         .withColumn("_source_file",   col("_metadata.file_path"))
# MAGIC         .withColumn("_ingested_date", lit(ingestedDate))
# MAGIC         .writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation",
# MAGIC             "abfss://bronze@streamingdata.dfs.core.windows.net/_checkpoint_json/")
# MAGIC         .outputMode("append")
# MAGIC         .option("mergeSchema", "true")
# MAGIC         .partitionBy("eventDate", "_ingested_date")
# MAGIC         .trigger(Trigger.AvailableNow())
# MAGIC         .start(Paths.bronzeTelemetry)
# MAGIC }
# MAGIC
# MAGIC println("Stream functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Parquet and JSON stream

# COMMAND ----------

# MAGIC %scala
# MAGIC println("=== Step 1: Ingesting Parquet files ===")
# MAGIC val parquetQuery = ingestParquetStream()
# MAGIC parquetQuery.awaitTermination()
# MAGIC println("Parquet stream complete")
# MAGIC
# MAGIC val afterParquet = spark.read.format("delta").load(Paths.bronzeTelemetry).count()
# MAGIC println(s"Bronze rows after parquet: $afterParquet")

# COMMAND ----------

# MAGIC %scala
# MAGIC println("=== Step 2: Ingesting JSON files ===")
# MAGIC val jsonQuery = ingestJsonStream()
# MAGIC jsonQuery.awaitTermination()
# MAGIC println("JSON stream complete")
# MAGIC
# MAGIC val afterJson = spark.read.format("delta").load(Paths.bronzeTelemetry).count()
# MAGIC println(s"Bronze rows after JSON: $afterJson")
# MAGIC spark.read.format("delta").load(Paths.bronzeTelemetry)
# MAGIC   .groupBy("eventDate").count().orderBy("eventDate").show()

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
# MAGIC //Stream functions
# MAGIC val q1 = ingestParquetStream()
# MAGIC q1.awaitTermination()
# MAGIC
# MAGIC val q2 = ingestJsonStream()
# MAGIC q2.awaitTermination()
# MAGIC
# MAGIC val countAfter = spark.read
# MAGIC   .format("delta")
# MAGIC   .load(Paths.bronzeTelemetry)
# MAGIC   .count()
# MAGIC
# MAGIC println(s"Row count AFTER rerun  : $countAfter")
# MAGIC println(s"Rows added             : ${countAfter - countBefore}")
# MAGIC
# MAGIC if (countBefore == countAfter)
# MAGIC   println("Idempotency confirmed — no duplicates on rerun")
# MAGIC else
# MAGIC   println(s"Count changed by ${countAfter - countBefore} rows")