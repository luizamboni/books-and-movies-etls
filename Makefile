clear:
	rm -rf ./data/raw/*
	rm -rf ./data/replication/*
	rm -rf ./data/silver/*
	rm -rf ./data/metastore/*

init_metastore:
	spark-submit jobs/engineer/setup/init_metastore.py

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
	spark-submit \
		--packages io.delta:delta-spark_2.12:3.0.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		jobs/engineer/database_replication/full-load.py \
		--job-name 'stream.users' \
		--origin-path $(shell pwd)/assets/data/users.csv \
		--destin-path $(shell pwd)/data/replication/users/

	spark-submit \
		--packages io.delta:delta-spark_2.12:3.0.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		jobs/engineer/database_replication/full-load.py \
		--job-name 'stream.movies' \
		--origin-path $(shell pwd)/assets/data/movies.csv \
		--destin-path $(shell pwd)/data/replication/movies/


	spark-submit \
		--packages io.delta:delta-spark_2.12:3.0.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		jobs/engineer/database_replication/full-load.py \
		--job-name 'stream.streams' \
		--origin-path $(shell pwd)/assets/data/streams.csv \
		--destin-path $(shell pwd)/data/replication/streams/


updates:
	spark-submit \
		--packages io.delta:delta-spark_2.12:3.0.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		jobs/analytics-engineer/transform-and-load.py \
		--job-name authors-transform-and-load \
		--origin-path-or-url $(shell pwd)/data/raw/authors/current.json \
		--destin-path $(shell pwd)/data/silver/users/ \
		-t 'metadata.name AS name' \
		-t 'metadata.birth_date AS birth_date' \
		-t 'metadata.died_at AS died_at' \
		-t 'transform(nationalities, x -> x.label) AS nationality_labels' \
		-pk name

	spark-submit \
		--packages io.delta:delta-spark_2.12:3.0.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		jobs/analytics-engineer/transform-and-load.py \
		--job-name books-transform-and-load \
		--origin-path-or-url $(shell pwd)/data/raw/authors/current.json \
		--destin-path $(shell pwd)/data/silver/books/ \
		-pk name

	spark-submit \
		--packages io.delta:delta-spark_2.12:3.0.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		jobs/analytics-engineer/transform-and-load.py \
		--job-name reviews-transform-and-load \
		--origin-path-or-url $(shell pwd)/data/raw/reviews/current.json \
		--destin-path $(shell pwd)/data/silver/reviews/ \
		-t 'content.text AS text' \
		-t 'rating.rate AS rate_value' \
		-t 'transform(books, x -> x.metadata.title) AS book_titles' \
		-t 'transform(movies, x -> x.title) AS movie_titles' \
		-t 'to_timestamp(created) AS created_at' \
		-pk created_at



run_all: clear incoming_data replication updates



pyspark:
	pyspark \
		--packages io.delta:delta-spark_2.12:3.0.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		--conf spark.sql.warehouse.dir=$(shell pwd)/data/metastore