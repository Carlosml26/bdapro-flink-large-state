#!/usr/bin/env bash
cd /home/danish/FastWorkspace/BDMA/TUB/BDAPRO/Project/bdapro-flink-large-state/data-producer-consumer/src/main/scripts
bash start-zookeeper.sh &
sleep 5
bash start-kafka.sh &
sleep 20
bash create-topics.sh