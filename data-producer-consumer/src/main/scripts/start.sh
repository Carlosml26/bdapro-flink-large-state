#!/usr/bin/env bash

cd /home/cmcuza/IdeaProjects/bdapro-flink-large-state/data-producer-consumer/src/main/scripts
export KAFKA_HOME="/usr/local/kafka/"
bash start-zookeeper.sh &
sleep 5
bash start-kafka.sh &
sleep 20
bash create-topics.sh