# Hive Streaming

## Architecture
Raw (ADLS Gen2) to Bronze (Delta) to Silver (Delta) to Dashboard

## Repository Structure

### databricks-notebooks/
Scala Spark pipeline notebooks running on Azure Databricks:
-'01_config_and_path' - Storage authentication and paths
-'02_schema_definition' - Case classes and StructType schema
-'03_raw_to_bronze'- Auto Loader ingestion with schema evolution
-'04_bronze_to_silver' - Buffering and quality metrics transformations
-'05_dashboard'- Quality of services visualisation

### hive-streaming-collector/
TypeScript HLS.js telemetry collector:
-'src/types.ts' - Data contracts
-'src/HiveStreamingCollector.ts' - Browser plugin
-'azure-function/index.ts' - Azure Function backend

### hive-streaming-pipeline/
Databricks Asset Bundle for pipeline orchestration:
-'databricks.yml' - Bundle configuration
-'resources/pipeline_job.yml' - Sequential job definition

## Live Endpoints
-Azure Function: https://hive-streaming-receiver-hedafehtdddpbkeg.centralindia-01.azurewebsites.net/api/telemetry
-Databricks Workspace: https://adb-7405610801529370.10.azuredatabricks.net

## Technologies
-Scala + Apache Spark + Delta Lake
-Azure Databricks + ADLS Gen2
-TypeScript + Azure Functions
-Databricks Asset Bundles