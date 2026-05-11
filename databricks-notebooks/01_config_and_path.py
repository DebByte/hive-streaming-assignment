# Databricks notebook source
# MAGIC %md
# MAGIC ###Test RAW container contents

# COMMAND ----------

# MAGIC %scala
# MAGIC val storageAccount = "streamingdata"
# MAGIC
# MAGIC val rawPath    = s"abfss://raw@$storageAccount.dfs.core.windows.net/"
# MAGIC val bronzePath = s"abfss://bronze@$storageAccount.dfs.core.windows.net/"
# MAGIC val silverPath = s"abfss://silver@$storageAccount.dfs.core.windows.net/"
# MAGIC val goldPath   = s"abfss://gold@$storageAccount.dfs.core.windows.net/"
# MAGIC
# MAGIC // Sub-paths
# MAGIC val bronzeTelemetry     = s"${bronzePath}telemetry/"
# MAGIC val silverBufferingPath = s"${silverPath}buffering_metrics/"
# MAGIC val silverQualityPath   = s"${silverPath}quality_metrics/"
# MAGIC val silverCheckpoint    = s"${silverPath}_checkpoint/"
# MAGIC
# MAGIC // Auto Loader internals
# MAGIC val autoloaderSchema     = s"${bronzePath}_autoloader_schema/"
# MAGIC val autoloaderCheckpoint = s"${bronzePath}_autoloader_checkpoint/"
# MAGIC
# MAGIC // Test
# MAGIC println("=== RAW container contents ===")
# MAGIC dbutils.fs.ls(rawPath).foreach(f => println(f.path))
# MAGIC
# MAGIC println("\n=== Paths ===")
# MAGIC println(s"bronzeTelemetry     : $bronzeTelemetry")
# MAGIC println(s"silverBufferingPath : $silverBufferingPath")
# MAGIC println(s"silverQualityPath   : $silverQualityPath")
# MAGIC println(s"silverCheckpoint    : $silverCheckpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Verify parquet files are accessible

# COMMAND ----------

# MAGIC %scala
# MAGIC val partitionPath = s"abfss://raw@streamingdata.dfs.core.windows.net/eventDate=2025-11-13/"
# MAGIC
# MAGIC val files = dbutils.fs.ls(partitionPath)
# MAGIC println(s"Total parquet files: ${files.size}")
# MAGIC
# MAGIC // Quick read to confirm data loads
# MAGIC val sampleDf = spark.read.parquet(partitionPath)
# MAGIC println(s"Total rows        : ${sampleDf.count()}")
# MAGIC println(s"Total columns     : ${sampleDf.columns.size}")
# MAGIC println(s"\nColumn names:")
# MAGIC sampleDf.columns.foreach(println)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Centralised path constants

# COMMAND ----------

# MAGIC %scala
# MAGIC object Paths {
# MAGIC   private val storage = "streamingdata"
# MAGIC   private def abfss(container: String) =
# MAGIC     s"abfss://$container@$storage.dfs.core.windows.net"
# MAGIC
# MAGIC   // Data layers
# MAGIC   val raw             = s"${abfss("raw")}/"
# MAGIC   val bronzeTelemetry = s"${abfss("bronze")}/telemetry/"
# MAGIC   val silverBuffering = s"${abfss("silver")}/buffering_metrics/"
# MAGIC   val silverQuality   = s"${abfss("silver")}/quality_metrics/"
# MAGIC   val gold            = s"${abfss("gold")}/"
# MAGIC
# MAGIC   // Auto Loader internals, inside bronze container, separate from data
# MAGIC   val autoloaderSchema     = s"${abfss("bronze")}/_autoloader_schema/"
# MAGIC   val autoloaderCheckpoint = s"${abfss("bronze")}/_autoloader_checkpoint/"
# MAGIC }
# MAGIC
# MAGIC println(s"raw              : ${Paths.raw}")
# MAGIC println(s"bronzeTelemetry  : ${Paths.bronzeTelemetry}")
# MAGIC println(s"silverBuffering  : ${Paths.silverBuffering}")
# MAGIC println(s"silverQuality    : ${Paths.silverQuality}")
# MAGIC println(s"checkpoint       : ${Paths.autoloaderCheckpoint}")
# MAGIC println(" Paths ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test: Printschema and nested columns

# COMMAND ----------

# MAGIC %scala
# MAGIC val rawDf = spark.read.parquet("abfss://raw@streamingdata.dfs.core.windows.net/eventDate=2025-11-13/")
# MAGIC rawDf.printSchema()
# MAGIC
# MAGIC // Test: nested columns
# MAGIC println("\n=== Sample qualityDistribution value ===")
# MAGIC rawDf.select("qualityDistribution").show(3, truncate = false)
# MAGIC
# MAGIC println("\n=== Sample player values ===")
# MAGIC rawDf.select("player").show(3, truncate = false)
# MAGIC
# MAGIC println("\n=== Sample timestampInfo values ===")
# MAGIC rawDf.select("timestampInfo").show(3, truncate = false)