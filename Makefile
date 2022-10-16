start-docker:
	docker-compose -f ./deploy/docker-compose.yml up -d

shutdown-docker:
	docker-compose -f ./deploy/docker-compose.yml down	

reset-volume-docker:
	sudo rm -rf ./deploy/docker/volumes/cassandra-seed/*
	sudo rm -rf ./deploy/docker/volumes/cassandra-node/*
	sudo rm -rf ./deploy/docker/volumes/kafka/*
	sudo rm -rf ./deploy/docker/volumes/zookeeper/*

setup-env:
	bash scripts/setup-env.sh

start-all:
	bash scripts/start-all.sh