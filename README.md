# Extending Great Expectations to integrate better with Databricks

[Great Expectations](https://greatexpectations.io/) is an amazing python library for data quality. It comes with integrations for Apache Spark, and dozens of preconfigured data expectations. Databricks is a top-tier data platform built on Spark. So you'd expect them to integrate seamlessly, but that is not quite the case…

This repo shows how you can extend the base great expectation dataset `SparkDFDataset` to achieve a better intergation with Databricks

## In this repo
- '**databrick_great_expectations**' : contains the extended class of `SparkDFDataset` with additional methods
  - `.validate_and_save()` : create the validation object, and saves the result as JSON to a storage location

- '**examples**' : contains 2 notebooks as examples on how to use the extended class and how to query the JSON data.
  - '**example_data_validation**' : shows how to use the extended class inline with PySpark code
  - '**example_query_results**' : shows how to use Databricks SQL to create a view of the JSON result data for additional querying

The accompanying medium article for the motiviation behind this repo can be read [here]()