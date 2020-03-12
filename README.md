# bdapro-flink-large-state
This repository provides the implementation of the businesscase for which we had to analyse event time and processing time latencies under high load.

We have one [Java producer](data-producer-consumer/src/main/java/org/dima/bdapro/datalayer/producer), one [Java Consumer](data-producer-consumer/src/main/java/org/dima/bdapro/datalayer/consumer). Both of them are in the same repo. The repo has it's own [README.md](data-producer-consumer/README.md) file which contains documentation regarding how to run the producer and the consumer.

We also have the implementation of the consumer in Flink in another [repo](large-state-dataprocessor). This project also has it's own [README.md](large-state-dataprocessor/README.md) which explains how to configure and run this project.

The monitoring tools used are Prometheus and Grafana. Corresponding prometheus configuration and Grafana dashboard can be found [here](data-producer-consumer/kafka-tools).
