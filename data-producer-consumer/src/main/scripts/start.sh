#!/usr/bin/env bash
bash start-zookeeper.sh &
sleep 5
bash start-kafka.sh &
sleep 20
bash create-topics.sh