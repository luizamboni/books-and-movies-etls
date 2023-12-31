SPARK_PAREMETERS=--packages io.delta:delta-spark_2.12:3.0.0 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		--conf spark.sql.warehouse.dir=$(shell pwd)/data/metastore \
		--conf spark.hadoop.hive.cli.print.header=true \
		--files ./log4j.properties \
		--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties


clear:
	rm -rf metastore_db
	rm -rf derby.log
	rm -rf ./data/raw/*
	rm -rf ./data/replication/*
	rm -rf ./data/silver/*
	rm -rf ./data/gold/*
	rm -rf ./data/metastore/*

init_metastore:
	spark-submit $(SPARK_PAREMETERS) \
		jobs/setup/init_metastore.py

incoming_data:
	python jobs/engineer/ftp/downloader.py \
		--origin-path-or-url '$(shell pwd)/assets/vendor/authors.json' \
		--destin-path '$(shell pwd)/data/raw/authors'
	python jobs/engineer/ftp/downloader.py \
		--origin-path-or-url '$(shell pwd)/assets/vendor/books.json' \
		--destin-path '$(shell pwd)/data/raw/books'
	python jobs/engineer/ftp/downloader.py \
		--origin-path-or-url '$(shell pwd)/assets/vendor/reviews.json' \
		--destin-path '$(shell pwd)/data/raw/reviews'

replication:
	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/database_replication/full-load.py \
		--table-name 'stream.users' \
		--origin-path $(shell pwd)/assets/data/users.csv \
		--destin-path $(shell pwd)/data/replication/users/

	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/database_replication/full-load.py \
		--table-name 'stream.movies' \
		--origin-path $(shell pwd)/assets/data/movies.csv \
		--destin-path $(shell pwd)/data/replication/movies/

	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/database_replication/full-load.py \
		--table-name 'stream.streams' \
		--origin-path $(shell pwd)/assets/data/streams.csv \
		--destin-path $(shell pwd)/data/replication/streams/


updates:
	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--table-name 'vendor.authors' \
		--origin-path-or-url $(shell pwd)/data/raw/authors/current.json \
		--destin-path $(shell pwd)/data/silver/users/ \
		-t 'metadata.name AS name' \
		-t 'metadata.birth_date AS birth_date' \
		-t 'metadata.died_at AS died_at' \
		-t 'transform(nationalities, x -> x.label) AS nationality_labels' \
		-pk name

	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--table-name 'vendor.books' \
		--origin-path-or-url $(shell pwd)/data/raw/books/current.json \
		--destin-path $(shell pwd)/data/silver/books/ \
		-pk name

	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--table-name 'vendor.reviews' \
		--origin-path-or-url $(shell pwd)/data/raw/reviews/current.json \
		--destin-path $(shell pwd)/data/silver/reviews/ \
		-t 'content.text AS text' \
		-t 'rating.rate AS rate_value' \
		-t 'transform(books, x -> x.metadata.title) AS book_titles' \
		-t 'transform(movies, x -> x.title) AS movie_titles' \
		-t 'to_timestamp(created) AS created_at' \
		-pk created_at


questions:
	spark-submit $(SPARK_PAREMETERS) \
		jobs/analytics-engineer/questions.py \
		--table-name 'analytics.movies_based_on_books' \
		--query-path $(shell pwd)/jobs/analytics-engineer/1-movies-based-on-books.sql \
		--destin-path $(shell pwd)/data/gold/movies-based-on-books/

	spark-submit $(SPARK_PAREMETERS) \
		jobs/analytics-engineer/questions.py \
		--table-name 'analytics.problem_with_unforgiven_in_christmas_2021' \
		--query-path $(shell pwd)/jobs/analytics-engineer/2-problem_with_unforgiven_in_christmas_2021.sql \
		--destin-path $(shell pwd)/data/gold/problem_with_unforgiven_in_christmas_2021/
	
	spark-submit $(SPARK_PAREMETERS) \
		jobs/analytics-engineer/questions.py \
		--table-name 'analytics.movies_based_on_singapurians_books_in_last_month' \
		--query-path $(shell pwd)/jobs/analytics-engineer/3-movies-based-on-singapurians-books-in-last-month.sql \
		--destin-path $(shell pwd)/data/gold/movies_based_on_singapurians_books_in_last_month/

	spark-submit $(SPARK_PAREMETERS) \
		jobs/analytics-engineer/questions.py \
		--table-name 'analytics.average_streamin_duration' \
		--query-path $(shell pwd)/jobs/analytics-engineer/4-average-streaming-duration.sql \
		--destin-path $(shell pwd)/data/gold/average_streamin_duration/


	spark-submit $(SPARK_PAREMETERS) \
		jobs/analytics-engineer/questions.py \
		--table-name 'analytics.median_streaming_size' \
		--query-path $(shell pwd)/jobs/analytics-engineer/5-median-streaming-size.sql \
		--destin-path $(shell pwd)/data/gold/median_streaming_size/

	spark-submit $(SPARK_PAREMETERS) \
		jobs/analytics-engineer/questions.py \
		--table-name 'analytics.how_many_users_watched_at_least_50_prercent_in_last_week' \
		--query-path $(shell pwd)/jobs/analytics-engineer/6-how-many-users-watched-at-least-50-prercent-in-last-week.sql \
		--destin-path $(shell pwd)/data/gold/how_many_users_watched_at_least_50_prercent_in_last_week/


run_all: clear init_metastore incoming_data replication updates questions



pyspark:
	pyspark $(SPARK_PAREMETERS)

sql:
	spark-sql $(SPARK_PAREMETERS)