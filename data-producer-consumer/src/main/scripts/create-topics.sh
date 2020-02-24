#!/usr/bin/env bash
source "`dirname $0`/environment.sh"
echo $N_PARTITIONS
${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper localhost:2181  --replication-factor 1 --partitions $N_PARTITIONS --topic HelloKafkaTopic1