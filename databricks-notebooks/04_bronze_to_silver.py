# Databricks notebook source
# MAGIC %run "/Users/debavijit.ad@gmail.com/hive_streaming/01_config_and_path"

# COMMAND ----------

# MAGIC %run "/Users/debavijit.ad@gmail.com/hive_streaming/02_schema_definition"

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
# MAGIC import io.delta.tables.DeltaTable
# MAGIC
# MAGIC println("Imports ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Bronze Stream

# COMMAND ----------

# MAGIC %scala
# MAGIC val bronzeStream = spark.readStream
# MAGIC   .format("delta")
# MAGIC   .load(Paths.bronzeTelemetry)
# MAGIC
# MAGIC println("Bronze stream defined")
# MAGIC println(s"Schema fields: ${bronzeStream.columns.mkString(", ")}")
# MAGIC //spark.read.format("delta").load(Paths.bronzeTelemetry).limit(10).show(false)

# COMMAND ----------

# MAGIC %md
# MAGIC ###SilverUpserts 

# COMMAND ----------

# MAGIC %scala
# MAGIC       
# MAGIC import io.delta.tables._
# MAGIC import org.apache.spark.sql.{DataFrame, SparkSession}
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC
# MAGIC object SilverUpserts extends Serializable {
# MAGIC     def computeBufferingMetrics(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
# MAGIC         val withServerTs = df.withColumn("serverTs", col("timestampInfo.server"))
# MAGIC         val deduped = withServerTs.dropDuplicates("clientId", "contentId", "customerId", "serverTs")
# MAGIC         val nullSafe = deduped.withColumn("bufferings",coalesce(col("player.bufferings"),lit(0)))
# MAGIC                           .withColumn("bufferingTime",coalesce(col("player.bufferingTime"), lit(0)))
# MAGIC         // Aggregate 
# MAGIC         val aggregatdf = nullSafe.groupBy( "clientId", "contentId", "customerId", "eventDate")
# MAGIC                                  .agg(sum(col("bufferings")).as("totalBufferings"),
# MAGIC                                       sum(col("bufferingTime")).cast("long").as("totalBufferingTimeMs"),
# MAGIC                                       count("*").as("snapshotCount"),
# MAGIC         // Session boundaries — derived from actual player timestamps
# MAGIC                                       min(col("timestampInfo.server")).as("sessionStartTs"),
# MAGIC                                       max(col("timestampInfo.server")).as("sessionEndTs"),
# MAGIC                                       max(col("_ingested_at")).as("lastIngestedAt")
# MAGIC                                       )
# MAGIC         val resultdf = aggregatdf.withColumn("avgBufferingTimeMs",
# MAGIC                                             when(col("totalBufferings") > 0, 
# MAGIC                                             col("totalBufferingTimeMs").cast("double") / col("totalBufferings").cast("double")
# MAGIC                                               ).otherwise(lit(0.0)))
# MAGIC         // Session duration from actual player timestamps (ms → sec)
# MAGIC                                  .withColumn("sessionDurationSec",
# MAGIC                                             when(col("sessionEndTs").isNotNull && col("sessionStartTs").isNotNull && col("sessionEndTs") > col("sessionStartTs"),
# MAGIC                                             (col("sessionEndTs") - col("sessionStartTs")) / lit(1000)
# MAGIC                                               ).otherwise(lit(0.0)))
# MAGIC         // Buffering % of session — relative QoS metric
# MAGIC                                  .withColumn("bufferingPct",
# MAGIC                                     when(col("sessionDurationSec") > 0,
# MAGIC                                       (col("totalBufferingTimeMs").cast("double") / lit(1000)) /
# MAGIC                                       col("sessionDurationSec") * lit(100)
# MAGIC                                     ).otherwise(lit(0.0)))
# MAGIC                                  .withColumn("poorQoS", col("totalBufferingTimeMs").gt(lit(5000)) .or(col("bufferingPct").gt(lit(5.0))))
# MAGIC                                  .withColumn("bufferingCategory",
# MAGIC                                         when(col("totalBufferings") === 0,           "No Buffering")
# MAGIC                                         .when(col("totalBufferings").between(1, 2),  "Low")
# MAGIC                                         .when(col("totalBufferings").between(3, 4),  "Medium")
# MAGIC                                         .otherwise(                                  "High")
# MAGIC                     )
# MAGIC         resultdf
# MAGIC                     }
# MAGIC     def computeQualityMetrics(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
# MAGIC
# MAGIC       val withServerTs = df.withColumn("serverTs", col("timestampInfo.server"))
# MAGIC
# MAGIC       val deduped = withServerTs.dropDuplicates("clientId", "contentId", "customerId", "serverTs")
# MAGIC
# MAGIC       val withValidMap = deduped.filter(col("qualityDistribution").isNotNull &&size(col("qualityDistribution")) > 0)
# MAGIC
# MAGIC       val exploded = withValidMap.select( col("clientId"), col("contentId"), col("customerId"), col("eventDate"), col("_ingested_at"),
# MAGIC                                     explode_outer(col("qualityDistribution"))
# MAGIC                                     .as(Seq("qualityLabel", "trafficData")))
# MAGIC
# MAGIC       val flattened = exploded.select(  col("clientId"), 
# MAGIC                                         col("contentId"), 
# MAGIC                                         col("customerId"), 
# MAGIC                                         col("eventDate"),
# MAGIC                                         col("qualityLabel"), 
# MAGIC                                         col("_ingested_at"),
# MAGIC                                         coalesce(col("trafficData.sourceTraffic.requests"), lit(0)).as("srcRequests"),
# MAGIC                                         coalesce(col("trafficData.sourceTraffic.responses"), lit(0.0)).as("srcResponses"),
# MAGIC                                         coalesce(col("trafficData.sourceTraffic.requestedData"), lit(0L)).as("srcRequestedBytes"),
# MAGIC                                         coalesce(col("trafficData.sourceTraffic.receivedData"), lit(0L)).as("srcReceivedBytes"),
# MAGIC                                         coalesce(col("trafficData.p2pTraffic.requests"), lit(0)).as("p2pRequests"),
# MAGIC                                         coalesce(col("trafficData.p2pTraffic.responses"), lit(0.0)).as("p2pResponses"),
# MAGIC                                         coalesce(col("trafficData.p2pTraffic.requestedData"), lit(0L)).as("p2pRequestedBytes"),
# MAGIC                                         coalesce(col("trafficData.p2pTraffic.receivedData"), lit(0L)).as("p2pReceivedBytes")
# MAGIC                                       )
# MAGIC
# MAGIC         val aggregated = flattened
# MAGIC                           .groupBy("clientId", "contentId", "customerId","eventDate", "qualityLabel")
# MAGIC                           .agg(
# MAGIC                             sum("srcRequests")       .as("totalSrcRequests"),
# MAGIC                             sum("srcResponses")      .as("totalSrcResponses"),
# MAGIC                             sum("srcRequestedBytes") .as("totalSrcRequestedBytes"),
# MAGIC                             sum("srcReceivedBytes")  .as("totalSrcReceivedBytes"),
# MAGIC                             sum("p2pRequests")       .as("totalP2PRequests"),
# MAGIC                             sum("p2pResponses")      .as("totalP2PResponses"),
# MAGIC                             sum("p2pRequestedBytes") .as("totalP2PRequestedBytes"),
# MAGIC                             sum("p2pReceivedBytes")  .as("totalP2PReceivedBytes"),
# MAGIC                             max("_ingested_at")      .as("lastIngestedAt")
# MAGIC                             )
# MAGIC         
# MAGIC         val withDerived = aggregated
# MAGIC                             .withColumn("totalReceivedBytes", col("totalSrcReceivedBytes") + col("totalP2PReceivedBytes"))
# MAGIC                             .withColumn("totalRequestedBytes", col("totalSrcRequestedBytes") + col("totalP2PRequestedBytes"))
# MAGIC                             .withColumn("p2pRatio",
# MAGIC                               when(col("totalReceivedBytes") > 0,
# MAGIC                                 col("totalP2PReceivedBytes").cast("double") /
# MAGIC                                 col("totalReceivedBytes").cast("double")
# MAGIC                               ).otherwise(lit(0.0)))
# MAGIC                             .withColumn("deliveryEfficiency",
# MAGIC                               when(col("totalRequestedBytes") > 0,
# MAGIC                                 col("totalReceivedBytes").cast("double") /
# MAGIC                                 col("totalRequestedBytes").cast("double")
# MAGIC                               ).otherwise(lit(0.0)))
# MAGIC                             .withColumn("dominantDelivery",
# MAGIC                               when(col("p2pRatio") > lit(0.5), "P2P")
# MAGIC                               .otherwise("Source"))
# MAGIC                             .withColumn("qualityRank",
# MAGIC                               when(col("qualityLabel") === "270p",  lit(1))
# MAGIC                               .when(col("qualityLabel") === "360p",  lit(2))
# MAGIC                               .when(col("qualityLabel") === "480p",  lit(3))
# MAGIC                               .when(col("qualityLabel") === "720p",  lit(4))
# MAGIC                               .when(col("qualityLabel") === "1080p", lit(5))
# MAGIC                               .when(col("qualityLabel") === "1440p", lit(6))
# MAGIC                               .when(col("qualityLabel") === "4K",    lit(7))
# MAGIC                               .otherwise(lit(0)))
# MAGIC                             .withColumn("isZeroTraffic",
# MAGIC                               col("totalReceivedBytes") === lit(0))
# MAGIC
# MAGIC         val viewerEventWindow = Window.partitionBy("clientId", "contentId", "customerId", "eventDate")
# MAGIC
# MAGIC         withDerived
# MAGIC             .withColumn("maxBytesForViewer", max(col("totalReceivedBytes")).over(viewerEventWindow))
# MAGIC             .withColumn("isDominantQuality", col("totalReceivedBytes") > lit(0) && col("totalReceivedBytes") === col("maxBytesForViewer"))
# MAGIC             .drop("maxBytesForViewer", "serverTs")
# MAGIC             }
# MAGIC
# MAGIC
# MAGIC     
# MAGIC     // Upsert Buffering
# MAGIC     def upsertBufferingMetrics( spark: SparkSession, batchDf: DataFrame, batchId: Long, silverPath: String ): Unit = {
# MAGIC         println(s"Batch $batchId : buffering metrics")
# MAGIC         val transformed = computeBufferingMetrics(batchDf)
# MAGIC
# MAGIC           if (!DeltaTable.isDeltaTable(spark, silverPath)) {
# MAGIC             transformed.write.format("delta")
# MAGIC               .mode("overwrite").partitionBy("eventDate")
# MAGIC               .save(silverPath)
# MAGIC             println(s"Batch $batchId : created buffering table")
# MAGIC           } else {
# MAGIC             DeltaTable.forPath(spark, silverPath).as("target")
# MAGIC               .merge(transformed.as("source"),
# MAGIC                 """target.clientId   = source.clientId   AND
# MAGIC                   target.contentId  = source.contentId  AND
# MAGIC                   target.customerId = source.customerId AND
# MAGIC                   target.eventDate  = source.eventDate""")
# MAGIC               .whenMatched().updateAll()
# MAGIC               .whenNotMatched().insertAll()
# MAGIC               .execute()
# MAGIC             println(s"Batch $batchId : upserted buffering metrics")
# MAGIC           }
# MAGIC           }
# MAGIC
# MAGIC       //Upsert Quality
# MAGIC     // Upsert Quality
# MAGIC     def upsertQualityMetrics( spark: SparkSession, batchDf: DataFrame, batchId: Long, silverPath: String ): Unit = {
# MAGIC           println(s"Batch $batchId — quality metrics")
# MAGIC           val transformed = computeQualityMetrics(batchDf)
# MAGIC
# MAGIC             if (!DeltaTable.isDeltaTable(spark, silverPath)) {
# MAGIC               transformed.write.format("delta")
# MAGIC                 .mode("overwrite").partitionBy("eventDate")
# MAGIC                 .save(silverPath)
# MAGIC               println(s"Batch $batchId : created quality table")
# MAGIC             } else {
# MAGIC               DeltaTable.forPath(spark, silverPath).as("target")
# MAGIC                 .merge(transformed.as("source"),
# MAGIC                   """target.clientId    = source.clientId    AND
# MAGIC                     target.contentId   = source.contentId   AND
# MAGIC                     target.customerId  = source.customerId  AND
# MAGIC                     target.eventDate   = source.eventDate   AND
# MAGIC                     target.qualityLabel = source.qualityLabel""")
# MAGIC                 .whenMatched().updateAll()
# MAGIC                 .whenNotMatched().insertAll()
# MAGIC                 .execute()
# MAGIC               println(s"Batch $batchId : upserted quality metrics")
# MAGIC             }
# MAGIC       }
# MAGIC   }
# MAGIC
# MAGIC   println("Silver Upserts object defined")

# COMMAND ----------

# %scala
# dbutils.fs.rm(Paths.silverBuffering + "_checkpoint/", recurse = true)
# dbutils.fs.rm(Paths.silverQuality   + "_checkpoint/", recurse = true)
# println("Checkpoints cleared")

# COMMAND ----------

# %scala
# dbutils.fs.rm("abfss://bronze@streamingdata.dfs.core.windows.net/_checkpoint_parquet/", recurse=true)
# dbutils.fs.rm("abfss://bronze@streamingdata.dfs.core.windows.net/_checkpoint_json/", recurse=true)
# dbutils.fs.rm("abfss://bronze@streamingdata.dfs.core.windows.net/_checkpoint_json_schema/", recurse=true)
# dbutils.fs.rm("abfss://bronze@streamingdata.dfs.core.windows.net/_autoloader_checkpoint/", recurse=true)
# dbutils.fs.rm("abfss://bronze@streamingdata.dfs.core.windows.net/_autoloader_schema/", recurse=true)
# dbutils.fs.rm("abfss://bronze@streamingdata.dfs.core.windows.net/_json_schema/", recurse=true)
# dbutils.fs.rm("abfss://silver@streamingdata.dfs.core.windows.net/_checkpoint/", recurse=true)
# println("All checkpoints cleared")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to Silver Delta Tables

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC
# MAGIC val bronzePath          = "abfss://bronze@streamingdata.dfs.core.windows.net/telemetry/"
# MAGIC val silverBufferingPath = "abfss://silver@streamingdata.dfs.core.windows.net/buffering_metrics/"
# MAGIC val silverQualityPath   = "abfss://silver@streamingdata.dfs.core.windows.net/quality_metrics/"
# MAGIC val silverCheckpoint    = "abfss://silver@streamingdata.dfs.core.windows.net/_checkpoint/"
# MAGIC
# MAGIC val query = spark.readStream
# MAGIC   .format("delta")
# MAGIC   .load(bronzePath)
# MAGIC   .writeStream
# MAGIC   .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
# MAGIC     println(s"=== Batch $batchId started ===")
# MAGIC     batchDf.cache()
# MAGIC     val batchSpark = batchDf.sparkSession
# MAGIC     SilverUpserts.upsertBufferingMetrics(
# MAGIC       batchSpark, batchDf, batchId, silverBufferingPath
# MAGIC     )
# MAGIC     SilverUpserts.upsertQualityMetrics(
# MAGIC       batchSpark, batchDf, batchId, silverQualityPath
# MAGIC     )
# MAGIC     batchDf.unpersist()
# MAGIC     println(s"=== Batch $batchId complete ===")
# MAGIC   }
# MAGIC   .option("checkpointLocation", silverCheckpoint)
# MAGIC   .trigger(Trigger.AvailableNow())
# MAGIC   .start()
# MAGIC
# MAGIC query.awaitTermination()
# MAGIC println("Bronze to Silver complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test

# COMMAND ----------

# DBTITLE 1,Cell 13
# MAGIC %scala
# MAGIC val silverBuf  = spark.read.format("delta").load(silverBufferingPath)
# MAGIC val silverQual = spark.read.format("delta").load(silverQualityPath)
# MAGIC
# MAGIC println(s"Silver buffering rows : ${silverBuf.count()}")
# MAGIC println(s"Silver quality rows   : ${silverQual.count()}")
# MAGIC
# MAGIC // Partition check
# MAGIC println("\n=== Buffering partitions ===")
# MAGIC silverBuf.groupBy("eventDate").count().orderBy("eventDate").show()
# MAGIC
# MAGIC println("\n=== Quality partitions ===")
# MAGIC silverQual.groupBy("eventDate").count().orderBy("eventDate").show()
# MAGIC
# MAGIC // Null check
# MAGIC import org.apache.spark.sql.functions.{col, count, when}
# MAGIC println("\n=== Null check — buffering ===")
# MAGIC silverBuf.select(
# MAGIC   count(when(col("totalBufferings").isNull,     1)).as("null_bufferings"),
# MAGIC   count(when(col("totalBufferingTimeMs").isNull,1)).as("null_bufferingTime"),
# MAGIC   count(when(col("poorQoS").isNull,             1)).as("null_poorQoS"),
# MAGIC   count(when(col("bufferingCategory").isNull,   1)).as("null_category")
# MAGIC ).show()
# MAGIC
# MAGIC // Sample
# MAGIC display(silverBuf.select(
# MAGIC   "clientId", "totalBufferings", "totalBufferingTimeMs",
# MAGIC   "bufferingPct", "poorQoS", "bufferingCategory"
# MAGIC ).orderBy(col("totalBufferingTimeMs").desc))