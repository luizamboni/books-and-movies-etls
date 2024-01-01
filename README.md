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

### DeltaTable

DeltaTable was used to deal with incremental changes without having to
rewrite the entire table and preventing their unavaibility.


# Phase 1
This phase was more fosused in engineer effort, a pipeline was build to get external data, keeping its history , never overwriting or erasing the content from previous days of raw data.
After that, other jobs take the new downloaded raw data and merge it with current tables. For that, DeltaTable was used.

`transform-and-load.py` works on raw data that previously came from `donwloader.py`, applying 
transformations and merging data before writing them on a Delta table.



# Phase 2

This analytics part was solved with some simple SQL queries that run on Spark engine, and write their results on specific tables.
The `questions.py` spark job was created for that.

CTE syntax was used in queries to make the them clearer.

1 - What percentage of the streamed movies are based on books ?

1.09 percent of streamed movies are beased on books.
by the query [/jobs/analytics-engineer/1-movies-based-on-books.sql](/jobs/analytics-engineer/1-movies-based-on-books.sql)

2 - During Christmas morning (7 am and 12 noon on December 25), a partial system outage was caused by a corrupted file. Knowing the file was part of the movie "Unforgiven" thus could affect any in-progress streaming session of that movie, how many users were potentially affected ?

Three users were affected.
by the query [/jobs/analytics-engineer/2-problem_with_unforgiven_in_christmas_2021.sql](/jobs/analytics-engineer/2-problem_with_unforgiven_in_christmas_2021.sql)


3 - How many movies based on books written by Singaporeans authors were streamed that month ?

Unfortunetely none.
By the query [/jobs/analytics-engineer/3-movies-based-on-singapurians-books-in-last-month.sql](/jobs/analytics-engineer/3-movies-based-on-singapurians-books-in-last-month.sql)

4 - What's the average streaming duration?

It is 722.26 minutes.
By the query [/jobs/analytics-engineer/4-average-streaming-duration.sql](/jobs/analytics-engineer/4-average-streaming-duration.sql)


5 - What's the **median** streaming size in gigabytes ?

The median is 0.94GB.
By the query [/jobs/analytics-engineer/5-median-streaming-size.sql](/jobs/analytics-engineer/5-median-streaming-size.sql)

6 - Given the stream duration (start and end time) and the movie duration, how many users watched at least 50% of any movie in the last week of the month (7 days) ?

234 users watched at least 50% of movies. 
I had to consider last day as the last day in streams.stream table, because the data only have until the 2021 year.
By que query [/jobs/analytics-engineer/6-how-many-users-watched-at-least-50-prercent-in-last-week.sql](/jobs/analytics-engineer/6-how-many-users-watched-at-least-50-prercent-in-last-week.sql)



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

