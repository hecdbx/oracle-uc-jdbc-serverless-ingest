# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle → Unity Catalog: ingest one table (full refresh)
# MAGIC
# MAGIC Reads one Oracle table through the Unity Catalog JDBC connection and overwrites the matching Delta table.
# MAGIC The job runs one task per table, all in parallel on serverless compute.
# MAGIC
# MAGIC - Arabic text (`VARCHAR2` / `CLOB`) arrives as UTF-8 strings without any conversion.
# MAGIC - Column names are lower-cased; `_ingested_at` and `_source` are added for lineage.
# MAGIC - Table maintenance (OPTIMIZE / VACUUM / statistics) is left to predictive optimization.

# COMMAND ----------

dbutils.widgets.text("connection", "oracle-jdbc-arabic", "UC JDBC connection")
dbutils.widgets.text("source_schema", "SYNTH_BANK", "Oracle schema")
dbutils.widgets.text("table", "", "Oracle table")
dbutils.widgets.text("target_catalog", "hec_demo_gcp", "Target catalog")
dbutils.widgets.text("target_schema", "oracle_datasets", "Target schema")

CONNECTION = dbutils.widgets.get("connection").strip()
SRC_SCHEMA = dbutils.widgets.get("source_schema").strip().upper()
TABLE = dbutils.widgets.get("table").strip().upper()
CATALOG = dbutils.widgets.get("target_catalog").strip()
SCHEMA = dbutils.widgets.get("target_schema").strip()
assert TABLE, "Set the table parameter"

# COMMAND ----------

from pyspark.sql import functions as F

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
except Exception as e:  # parallel tasks can race on the first run
    if "SCHEMA_ALREADY_EXISTS" not in str(e):
        raise

df = (spark.read.format("jdbc")
      .option("databricks.connection", CONNECTION)
      .option("dbtable", f'"{SRC_SCHEMA}"."{TABLE}"')
      .load())
df = (df.toDF(*[c.lower() for c in df.columns])
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(f"oracle:{SRC_SCHEMA}.{TABLE}")))

target = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE.lower()}`"
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)

rows = spark.table(target).count()
print(f"{SRC_SCHEMA}.{TABLE} -> {target}: {rows:,} rows")
dbutils.notebook.exit(str(rows))
