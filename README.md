Books and movies inteligence
===

## Get started
To run all

First install the dependencies
```shell
$ pip install -r requirements.txt
```

And then run all the jobs.
```shell
$ make pipeline
```
Obs.: There are others tasks, you can check it on Makefile


### Technologies

In this project the following technologies were used:

- Python
- Spark (pyspark)
- DeltaTable


All of this was ran in a local machine (mac M1), but it can be reproduced in a preferable vendor cloud or in on-premise infraestructure.

### **Python**  
(pure Python) 

It was used to build the `downloader.py` tool that is responsible for bringing the data from out to in DataLake, in its raw format.

### **PySpark** 

It was used to build ELT tools that works with data already in DataLake. 

`full-load.py` that refreshes the tables (mocked here as csv files) and outputs a table in a Delta format.

`transform-and-load.py` works on raw data that previously came from `donwloader.py`, applying 
transformations and merging data before writing them on a Delta table.

### DeltaTable

DeltaTable was used to deal with incremental changes without having to
rewrite the entire table and preventing their unavaibility.


# Phase 1
This phase was more fosused in engineer effort, a pipeline was build to get external data, keeping its history , never overwriting or erasing the content from previous days of raw data.
After that, other jobs take the new downloaded raw data and merge it with current tables. For that, DeltaTable was used.

# Phase 2

This analytics part was solved with some simple SQL queries that run on Spark engine, and write their results on specific tables.
The `questions.py` spark job was created for that.

CTE syntax was used in queries to make the them clearer.


# Phase 3
For production this pipeline should have some small changes, and sometimes it depepends on what Cloud 
the clients will choose or even if they prefer on-premisses servers.
One of the main changes is use a Cloud metastorage as AWS Glue or Databricks Unity catalog.

My idea is to implement a mix of Databricks and AWS.

The base technologies remains the same. Python, Pyspark and DeltaTable.

Firstly, Terraform with Gitlab CI ou other CI based on git repo for Continuous Integration.

Databricks Sql Endpoint could be used to integrate the data with some dataviz tool like Metabase or Superset. 
It is just turn on, no big deal.

As storage layer, I recomend S3, due the big number of tools with integration as its many options of
applicable policies, like retention period, moving policies, access and its low price.

The tables write and merge jobs could be done at regular intervals - hourly, I guess - for the tables based in our applications and once a day for tables based on external data that comes also once a day. 
In Databricks it can be scheduled easily even with dependent jobs.

Despite de amount of data that comes every day (8GB) be apparently a huge ammount - specially when we multiply it
for 365 days, which would give us 2.92TB - we are always working with diferencials. Then it turns in a small data.

A policy for discarding old data can be implemented, using a backup of the tables' last state. The raw downloaded data have its utility at maximum, in 30-90 days roughly, when it is useful to have this data to check some inconsistencies and drilldown sobre incidents. 

![Data Pipeline](/architecture.png "Architecture")

