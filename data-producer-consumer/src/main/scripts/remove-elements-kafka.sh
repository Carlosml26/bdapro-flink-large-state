#!/usr/bin/env bash
source "`dirname $0`/environment.sh"
${KAFKA_HOME}/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic HelloKafkaTopic1