# Databricks notebook source


# COMMAND ----------



# COMMAND ----------

!pip install great_expectations==0.15.0

# COMMAND ----------

from databricks_great_expectations.extended_dataset import ExtendedSparkDFDataset

# COMMAND ----------

# Load taxi data but limit it to 10_000 records for this example
df_taxi = (
    spark
        .sql('SELECT * FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`')
        .limit(10_000)
)

# COMMAND ----------

# Create the GE object using the ExtendedSparkDFDataset class
ge_df_taxi = ExtendedSparkDFDataset(df_taxi, dbutils)

# COMMAND ----------

# DBTITLE 0,Successful Validation
# validate and save
ge_df_taxi.validate_and_save()

# COMMAND ----------

# Create the GE object using the ExtendedSparkDFDataset class
ge_df_taxi = ExtendedSparkDFDataset(df_taxi, dbutils)

# COMMAND ----------

# DBTITLE 0,Unsuccessful Validation
ge_df_taxi.validate_and_save()
