#!/usr/bin/env bash
<<<<<<< HEAD
export KAFKA_HOME="/usr/local/kafka/"
=======
cd /home/danish/FastWorkspace/BDMA/TUB/BDAPRO/Project/bdapro-flink-large-state/data-producer-consumer/src/main/scripts
>>>>>>> flink-consumer
bash start-zookeeper.sh &
sleep 5
bash start-kafka.sh &
sleep 20
bash create-topics.sh