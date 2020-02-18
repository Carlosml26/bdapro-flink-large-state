#!/usr/bin/env bash
# cd to scripts folder.
export KAFKA_HOME="/usr/local/kafka/"

bash start-zookeeper.sh &
sleep 5
bash start-kafka.sh &
sleep 20
bash create-topics.sh