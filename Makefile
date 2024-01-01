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

raw:
	python jobs/engineer/ftp/downloader.py \
		--origin-path-or-url '$(shell pwd)/assets/vendor/authors.json' \
		--destin-path '$(shell pwd)/data/raw/vendor/authors'

	python jobs/engineer/ftp/downloader.py \
		--origin-path-or-url '$(shell pwd)/assets/vendor/books.json' \
		--destin-path '$(shell pwd)/data/raw/vendor/books'

	python jobs/engineer/ftp/downloader.py \
		--origin-path-or-url '$(shell pwd)/assets/vendor/reviews.json' \
		--destin-path '$(shell pwd)/data/raw/vendor/reviews'

	python jobs/engineer/ftp/downloader.py \
		--origin-path-or-url '$(shell pwd)/assets/data/streams.csv' \
		--destin-path '$(shell pwd)/data/raw/stream/streams'

	python jobs/engineer/ftp/downloader.py \
		--origin-path-or-url '$(shell pwd)/assets/data/users.csv' \
		--destin-path '$(shell pwd)/data/raw/stream/users'

	python jobs/engineer/ftp/downloader.py \
		--origin-path-or-url '$(shell pwd)/assets/data/movies.csv' \
		--destin-path '$(shell pwd)/data/raw/stream/movies'

silver:
	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--overwrite \
		--table-name 'stream.streams' \
		--origin-path-or-url $(shell pwd)/data/raw/stream/streams/current.csv \
		--destin-path $(shell pwd)/data/silver/stream/streams/ \
		-pk start_at \
		-t 'to_timestamp(start_at) AS start_at' \
		-t 'to_timestamp(end_at) AS end_at' \
		-t 'lower(movie_title) AS movie_title' \
		-t 'user_email' \
		-t 'size_mb'

	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--overwrite \
		--table-name 'stream.users' \
		--origin-path-or-url $(shell pwd)/data/raw/stream/users/current.csv \
		--destin-path $(shell pwd)/data/silver/stream/users/ \
		-pk first_name \
		-pk last_name \
		-t 'lower(first_name) AS first_name' \
		-t 'lower(last_name) AS last_name' \
		-t 'email'

	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--overwrite \
		--table-name 'stream.movies' \
		--origin-path-or-url $(shell pwd)/data/raw/stream/movies/current.csv \
		--destin-path $(shell pwd)/data/silver/stream/movies/ \
		-pk title \
		-t 'lower(title) AS title' \
		-t 'duration_mins' \
		-t 'lower(original_language) AS original_language' \
		-t size_mb

# from vendor
	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--table-name 'vendor.authors' \
		--origin-path-or-url $(shell pwd)/data/raw/vendor/authors/current.json \
		--destin-path $(shell pwd)/data/silver/vendor/authors/ \
		-t 'lower(metadata.name) AS name' \
		-t 'to_timestamp(metadata.birth_date) AS birth_date' \
		-t 'to_timestamp(metadata.died_at) AS died_at' \
		-t 'transform(nationalities, x -> lower(x.label)) AS nationality_labels' \
		-pk name

	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--table-name 'vendor.books' \
		--origin-path-or-url $(shell pwd)/data/raw/vendor/books/current.json \
		--destin-path $(shell pwd)/data/silver/vendor/books/ \
		-pk name \
		-t 'lower(name) AS name' \
		-t 'pages' \
		-t 'lower(author) AS author' \
		-t 'lower(publisher) AS publisher'


	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--table-name 'vendor.reviews' \
		--origin-path-or-url $(shell pwd)/data/raw/vendor/reviews/current.json \
		--destin-path $(shell pwd)/data/silver/vendor/reviews/ \
		-t 'lower(content.text) AS text' \
		-t 'rating.rate AS rate_value' \
		-t 'transform(books, x -> lower(x.metadata.title)) AS book_titles' \
		-t 'transform(movies, x -> lower(x.title)) AS movie_titles' \
		-t 'to_timestamp(created) AS created_at' \
		-pk created_at


gold:
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
		--table-name 'analytics.movies_based_on_singapurians_books_in_december_2021' \
		--query-path $(shell pwd)/jobs/analytics-engineer/3-movies-based-on-singapurians-books-in-december-2021.sql \
		--destin-path $(shell pwd)/data/gold/movies_based_on_singapurians_books_in_december_2021/

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


pipeline: clear init_metastore raw silver gold

pyspark:
	pyspark $(SPARK_PAREMETERS)

sql:
	spark-sql $(SPARK_PAREMETERS)


dev:
	spark-submit $(SPARK_PAREMETERS) \
		jobs/engineer/transform-and-load.py \
		--overwrite \
		--table-name 'stream.streams_test' \
		--origin-path-or-url $(shell pwd)/assets/data/streams.csv \
		--destin-path $(shell pwd)/data/silver/stream/streams_test \
		-pk start_at \
		-t 'to_timestamp(start_at) AS start_at' \
		-t 'to_timestamp(end_at) AS end_at' \
		-t 'lower(movie_title) AS movie_title' \
		-t 'user_email' \
		-t 'size_mb'

