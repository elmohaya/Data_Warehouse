# Data Warehouse

## Background Information
In this project we model data related to a music streaming startup, Sparkify. their data resides in S3 where we will stage them next into Redshift. Thus, their analytics team can easily query and analyze the data efficiently in the cloud of Amazon Web Services. The aim is to design and build a schema that is hold on a Redshift cluster on AWS.

## Objective
In this project we aim to create an ETL pipeline that can extract data from S3, stage the data in Redshift, and transform it into a set of dimensional tables. This process provides easier environment for executing queries and looking for insights in the data.

This project consists of five files: 

1-  sql_queries.py -- here we write all the queries that involve creating, dropping, and inserting data into tables.

2-  create_tables.py -- in this file dropping and creating queries are executed respectively.

3-  etl.py -- here we load the data from S3 and then distribute it into the schema tables in the DB

4-  dwh.cfg -- this file contains the information and access key that enable establishing the connection to the Redshift Cluster.

5-  README.md -- This file!


## How to Run The Code
First, create_tables file is run such that it can drop all previous tables and create them again by executing the list of queries written in sql_queries. Afterthat, etl.py file is run such that the staging face is active which will activate the insertion face next. Hence, our dimentional tables will be created in the cloud.