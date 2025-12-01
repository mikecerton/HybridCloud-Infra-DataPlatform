from pyspark.sql.functions import col, when, length, expr, lit
from pyspark.sql import DataFrame
from loguru import logger
spark.conf.set("spark.sql.session.timeZone", "Asia/Bangkok")


"""
---------------- Configuration ----------------
"""

# azure_storage_account_name  EX: project_name_dev, project_name_prod
STORAGE_ACCOUNT_NAME = "project_Name_dev"
# container_name in azure_storage_account EX: ProjectName
CONTAINER_NAME = "projectName"
# csv file name EX: sales_data.csv, customer_data.csv
file_name = "file_name.csv
# metadata file name EX: sales_data_metadata.csv, customer_data_metadata.csv
metadata_file_name = "file_name_metadata.csv"

# databricks_catalog_name  EX: project_name_dev, project_name_prod
dbt_catalog_name = "projctname_dev"
# databricks_schema_name EX: silver, gold
dbt_schema_name = "schema_name"
# databricks_table_name
dbt_table_name = "table_name"
# full_table_path
full_table_path = f"{dbt_catalog_name}.{dbt_schema_name}.{dbt_table_name}"

# initial file_path and metadata_path
file_path = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_name}"
metadata_path = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{metadata_file_name}"

system_process_datetime_bkk = F.from_utc_timestamp(F.current_timestamp(), "Asia/Bangkok")


"""
---------------- Read CSV ----------------
"""

try:
   logger.info(f"Reading CSV from {file_path}")

   df = (
       spark
       .read
       .format("csv")
       .options(
           header="true",
           delimiter=",",
           inferSchema="true",
           multiLine="true",
           quote='"',
           escape='"'
       )
       .load(file_path)
   )

   logger.success("Finished reading CSV")

except Exception as e:
   logger.error(f"Error reading CSV: {e}")
   raise


try:
   logger.info(f"Reading metadata CSV from {file_path}")

   metadata_df = (
       spark
       .read
       .format("csv")
       .options(
           header="true",
           delimiter=",",
           inferSchema="true",
           multiLine="true",
           quote='"',
           escape='"'
       )
       .load(metadata_path)
   )

   logger.success("Finished reading CSV")

except Exception as e:
   logger.error(f"Error reading CSV: {e}")
   raise


"""
---------------- Metadata Quality Check ----------------
"""
#   - Validate metadata definition from metadata CSV
#   - Ensure required columns exist in data
#   - Ensure data types match metadata specification
#   - Ensure column-level constraints (nullable, length, etc.)

try:
    logger.info("Checking rows count")
    if metadata_df.select("record_count").first()[0] != df.count():
        raise ValueError(f"Invalid row count: {metadata_df.select("record_count").first()[0]}, expected: {df.count()}")

    logger.info("Checking column list")
    metadata_column_list = metadata_df.select("column_list").first()[0].split(",")
    actual_column_list = df.columns
    if metadata_column_list != actual_column_list:
        raise ValueError(f"Invalid column list: {metadata_column_list}, expected: {actual_column_list}")

except Exception as e:
 logger.error(f"Error checking metadata: {e}")
 raise


"""
---------------- Data Transformation ----------------
"""
#   - do your data transformation here

try:
    # add process datetime column
    logger.info("Adding system_process_datetime_bkk")
    df = df.withColumn("system_process_datetime_bkk", system_process_datetime_bkk)

except Exception as e:
    logger.error(f"Error checking metadata: {e}")
    raise


"""
---------------- Data Quality ----------------
"""

logger.info("Checking data quality ...")

dq_df = df.withColumn(
   "dq_errors",
   expr(
       """
   CASE WHEN record_id IS NULL OR length(record_id) < 8 THEN 'Invalid record_id; '
        ELSE ''
   END ||
   CASE WHEN item_id IS NULL THEN 'Missing item_id; '
        ELSE ''
   END ||
   CASE WHEN amount < 0 THEN 'Negative amount; '
        ELSE ''
   END ||
   CASE WHEN value_unit < 0 THEN 'Invalid value_unit; '
        ELSE ''
   END ||
"""
   ),
)

failing_df = dq_df.filter(col("dq_errors") != "")

error_count = failing_df.count()
logger.info(f"Found {error_count} rows with data quality issues")

if error_count > 0:
    failing_df.display()
    raise Exception(f"Data Quality Check Failed! Total failing rows: {error_count}")

else:
   logger.success("Data Quality Check Passed: No issues found.")

"""
---------------- Write as delta table to Databricks (overwrite or upsert) ----------------
"""

try: 
   """---------  overwrite table  ---------"""
   (
   df
   .write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "false")
   .saveAsTable(full_table_path)
   )
   """------------------------------------"""

   """---------  upsert table  ---------"""
   delta = DeltaTable.forName(spark, full_table_path)
   merge_condition = "target.id = source.id"
   (
       delta.alias('target')
       .merge(
           source=df.alias('source'),
           condition=merge_condition
       )
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
   )
   """----------------------------------"""
   logger.success(f"Finished writing table {full_table_path}")

except Exception as e:
   logger.error(f"Error writing table: {e}")
   raise