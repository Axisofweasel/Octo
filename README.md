# Octopus Energy ETL Project
This project demonstrates an Extract, Transform, and Load (ETL) process from the Octopus Energy API using PySpark for data processing and staging in Azure Data Lake Storage. The final step involves transforming and loading the data into a Dockerized MS-SQL Server instance as a proof of concept.

Overview
The primary goal of this project is to:

Extract: Fetch consumption data from the Octopus Energy API and save the raw files to ADLS GEN 2 using Spark.
Transform: Process and clean the data using PySpark.
Load: Stage the transformed data into Azure Data Lake Storage and subsequently load it into a Dockerized MS-SQL Server instance as a dimensional model.
Serve: Maybe to a dockerised grafana instance or maybe to Raspberry Pi screen

Octopus API [Docs](https://developer.octopus.energy/rest/guides/endpoints)

Currently a work in progress.
