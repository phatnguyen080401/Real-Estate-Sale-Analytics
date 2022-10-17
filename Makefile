start-docker:
	docker-compose -f ./deploy/docker-compose.yml up -d
	mkdir ./src/checkpoint/

shutdown-docker:
	docker-compose -f ./deploy/docker-compose.yml down
	sudo rm -rf ./deploy/docker/volumes/kafka/meta.properties
	sudo rm -rf ./src/checkpoint/

reset-volume-docker:
	sudo rm -rf ./deploy/docker/volumes/cassandra-seed/*
	sudo rm -rf ./deploy/docker/volumes/cassandra-node/*
	sudo rm -rf ./deploy/docker/volumes/kafka/*
	sudo rm -rf ./deploy/docker/volumes/zookeeper/*

setup-env:
	bash scripts/setup-env.sh

start-all:
	bash scripts/start-all.sh