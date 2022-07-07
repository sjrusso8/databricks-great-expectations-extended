# Databricks notebook source
# MAGIC %sql
# MAGIC COPY INTO delta.`/data-quality-results/`
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       *,
# MAGIC       current_timestamp() as `load_timestamp`,
# MAGIC       input_file_name() as `source_file_name`
# MAGIC     FROM '/data-quality/*/*/*/*/*/*'
# MAGIC   )
# MAGIC   FILEFORMAT = JSON
# MAGIC   FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC   COPY_OPTIONS   ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH results_explode as (
# MAGIC   SELECT
# MAGIC     meta.batch_kwargs.ge_batch_id as batch_id,
# MAGIC     timestamp(meta.run_id.run_time) as run_time_id,
# MAGIC     meta.expectation_suite_meta.citations[0].comment.extraContext.notebook_path as notebook_path,
# MAGIC     meta.expectation_suite_meta.citations[0].comment.tags.clusterId as cluster_id,
# MAGIC     explode(results) as results
# MAGIC   FROM
# MAGIC     delta.`/data-quality-results/`
# MAGIC )
# MAGIC SELECT 
# MAGIC   batch_id,
# MAGIC   cluster_id,
# MAGIC   results.expectation_config.expectation_type as expectation_type,
# MAGIC   IFF(results.success, 'Pass', 'Fail') as success_status,
# MAGIC   results.*,
# MAGIC   results.expectation_config.kwargs.*  
# MAGIC FROM results_explode;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW default.data_quality_header AS
# MAGIC SELECT 
# MAGIC   meta.batch_kwargs.ge_batch_id as batch_id,
# MAGIC   timestamp(meta.run_id.run_time) as run_time_id,
# MAGIC   meta.expectation_suite_meta.citations[0].comment.extraContext.notebook_path as notebook_path,
# MAGIC   IFF(success, 'Pass', 'Fail') as pass_or_fail,
# MAGIC   `statistics`.evaluated_expectations as evaulated_expectations,
# MAGIC   format_number(`statistics`.success_percent/100, '0%') as success_percent,
# MAGIC   `statistics`.successful_expectations as successful_expectations,
# MAGIC   `statistics`.unsuccessful_expectations as unsuccessful_expectations 
# MAGIC FROM delta.`/data-quality-results/`;
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW default.data_quality_body AS
# MAGIC WITH results_explode as (
# MAGIC   SELECT
# MAGIC     meta.batch_kwargs.ge_batch_id as batch_id,
# MAGIC     timestamp(meta.run_id.run_time) as run_time_id,
# MAGIC     meta.expectation_suite_meta.citations[0].comment.extraContext.notebook_path as notebook_path,
# MAGIC     explode(results) as results
# MAGIC   FROM
# MAGIC     delta.`/data-quality-results/`
# MAGIC )
# MAGIC SELECT 
# MAGIC   batch_id,
# MAGIC   run_time_id,
# MAGIC   notebook_path,
# MAGIC   IFF(results.success, 'Pass', 'Fail') as success_status,
# MAGIC   results.expectation_config.expectation_type as expectation_type,
# MAGIC   results.result.*,
# MAGIC   results.expectation_config.kwargs.*  
# MAGIC FROM results_explode;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.data_quality_header

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_quality_body
