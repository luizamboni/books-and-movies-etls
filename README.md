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





