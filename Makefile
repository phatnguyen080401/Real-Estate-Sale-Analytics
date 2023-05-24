build-image:
	docker build -t custom-airflow ./src

start-kafka:
	docker-compose -f ./deploy/docker-compose.yml up -d

shutdown-kafka:
	docker-compose -f ./deploy/docker-compose.yml down
	sudo rm -rf ./deploy/docker/volumes/kafka/meta.properties

start-airflow:
	docker-compose -f ./deploy/apache-airflow.yml up -d

shutdown-airflow:
	docker-compose -f ./deploy/apache-airflow.yml down

reset-volume-docker:
	sudo rm -rf ./deploy/docker/volumes/airflow/*
	sudo rm -rf ./deploy/docker/volumes/postgres/*
	sudo rm -rf ./deploy/docker/volumes/kafka/*
	sudo rm -rf ./deploy/docker/volumes/zookeeper/*

setup:
	bash scripts/setup.sh