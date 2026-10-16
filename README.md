# Oracle → Unity Catalog with serverless jobs and a custom JDBC connection

A minimal example of batch-ingesting Oracle tables into Unity Catalog with **serverless Lakeflow Jobs**. It uses a **Unity Catalog JDBC connection** that loads the Oracle JDBC driver JARs (`ojdbc11.jar`, `orai18n.jar`) from a UC volume.

It was tested against Oracle 19c on Amazon RDS, with Arabic text in `VARCHAR2` and `CLOB` columns, landing in Delta tables on a Databricks serverless workspace.

## How it works

- `src/ingest_table.py` reads **one** Oracle table through the connection and overwrites the matching Delta table (full refresh, no CDC):
  ```python
  spark.read.format("jdbc")
       .option("databricks.connection", "<connection-name>")
       .option("dbtable", '"SCHEMA"."TABLE"')
       .load()
  ```
- `resources/oracle_ingest.job.yml` defines a job with **one task per table** and no dependencies between them, so all tables load in parallel on serverless compute. Each task has its own status, logs and retry.
- Column names are lower-cased, and `_ingested_at` and `_source` columns are added. Table maintenance is left to predictive optimization.

## Prerequisites

1. **Network:** Oracle is reachable from Databricks serverless compute (allow TCP 1521 from your region's serverless egress IPs, or use private connectivity).
2. **JDBC driver:** `ojdbc11.jar` and `orai18n.jar`, from the same Oracle JDBC release, are uploaded to a UC volume.
3. **UC JDBC connection:** created with
   - URL `jdbc:oracle:thin:@//<host>:1521/<service>`
   - both JARs as Java dependencies
   - `user` and `password` under **Additional options**
   - an external options allow list that includes `dbtable`
4. **Permissions:** the job's run-as identity has `USE CONNECTION`, `READ VOLUME` on the JAR volume, and `CREATE TABLE` on the target schema.

## Deploy and run

```bash
databricks bundle deploy --profile <profile>
databricks bundle run oracle_synth_bank_ingest --profile <profile>
```

Override the job parameters for your environment, either as defaults in `resources/oracle_ingest.job.yml` or at run time:

| Parameter | Example default |
|---|---|
| `connection` | `oracle-jdbc-arabic` |
| `source_schema` | `SYNTH_BANK` |
| `target_catalog` | `hec_demo_gcp` |
| `target_schema` | `oracle_datasets` |

To add a table, copy a task block in `resources/oracle_ingest.job.yml` and change `task_key` and `table`.
