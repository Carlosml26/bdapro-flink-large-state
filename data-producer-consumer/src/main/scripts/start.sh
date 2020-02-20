#!/usr/bin/env bash

#change this to the place where kafka is in your PC
export KAFKA_HOME="/usr/local/kafka/"
#change this to your own root dir
export ROOT_DIR="/home/cmcuza/IdeaProjects/bdapro-flink-large-state/"
cd data-producer-consumer/src/main/scripts
bash start-zookeeper.sh &
sleep 5
bash start-kafka.sh &
sleep 20
bash create-topics.sh