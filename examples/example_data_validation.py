# Databricks notebook source
# Ensure that Great Expectations is installed
!pip install great_expectations==0.16.3

# COMMAND ----------

# Import the ExtendedSparkDFDataset
# The Databricks feature 'Files in Repo' allows for local imports of python files
from databricks_great_expectations.extended_dataset import ExtendedSparkDFDataset

# COMMAND ----------

# Load taxi data but limit it to 10_000 records for this example
df_taxi = (
    spark
        .sql('SELECT * FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`')
        .limit(10_000)
)

# COMMAND ----------

# Create a GE DataFrame using new ExtendedSparkDFDataset 
# Passing the df_taxi dataframe and dbutils

ge_df_taxi = ExtendedSparkDFDataset(df_taxi, dbutils)

# COMMAND ----------

# DBTITLE 0,Successful Validation
# Write data expectation tests using the GE DataFrame

# Create table level expectations
ge_df_taxi.expect_table_row_count_to_equal(10_000)
ge_df_taxi.expect_table_column_count_to_equal(18)

# Create column level expectations
ge_df_taxi.expect_column_values_to_not_be_null('vendor_id')
ge_df_taxi.expect_column_distinct_values_to_be_in_set('vendor_id', ['VTS', 'CMT', 'DDS'])
ge_df_taxi.expect_column_values_to_be_of_type('pickup_datetime', 'TimestampType')
ge_df_taxi.expect_column_values_to_be_of_type('dropoff_datetime', 'TimestampType')
ge_df_taxi.expect_column_max_to_be_between('passenger_count', min_value=1, max_value=6)
ge_df_taxi.expect_column_max_to_be_between('fare_amount', min_value=1, max_value=200)

# Call the new validate_and_save() method
ge_df_taxi.validate_and_save()

# COMMAND ----------

# Create the GE object using the ExtendedSparkDFDataset class
ge_df_taxi = ExtendedSparkDFDataset(df_taxi, dbutils)

# COMMAND ----------

# DBTITLE 0,Unsuccessful Validation
# Table level tests
ge_df_taxi.expect_table_row_count_to_equal(1000)
ge_df_taxi.expect_table_column_count_to_equal(1)

# Column level tests
ge_df_taxi.expect_column_values_to_be_null('vendor_id')
ge_df_taxi.expect_column_values_to_be_of_type('pickup_datetime', 'StringType')
ge_df_taxi.expect_column_values_to_be_of_type('dropoff_datetime', 'StringType')
ge_df_taxi.expect_column_max_to_be_between('passenger_count', min_value=1, max_value=4)
ge_df_taxi.expect_column_max_to_be_between('fare_amount', min_value=1, max_value=75)

# validate and save
ge_df_taxi.validate_and_save()
