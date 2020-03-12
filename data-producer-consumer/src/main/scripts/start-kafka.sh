#!/usr/bin/env bash
source "`dirname $0`/environment.sh"
#be careful with the following it depends on RAM.
export KAFKA_HEAP_OPTS="-Xmx6g"

# Use the following to enable KAFKA monitoring through prometheus
# For this jmx_prometheus_javaagent jar should be placed in $KAFKA_HOME
export KAFKA_OPTS="-javaagent:$KAFKA_HOME/jmx_prometheus_javaagent-0.12.0.jar=7071:$KAFKA_HOME/kafka-2_0_0.yml" \

${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties