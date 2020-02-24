#!/usr/bin/env bash
source "`dirname $0`/environment.sh"
cd "`dirname $0`"
bash start-zookeeper.sh &
sleep 5
bash start-kafka.sh &
sleep 5
bash create-topics.sh