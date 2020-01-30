# Data Producer and Single Node Java Producer
## Setup instructions
Setup KAFKA_HOME environment variable.

```shell script
export KAFKA_HOME="path/to/kafka_xx_xx"
```

Run the following three scripts

```shell script
scripts/start-zookeeper.sh
scripts/start-kafka.sh
scripts/create-topics.sh
```

## Data Generator
### Configuration

Change the config in this [file](src/main/conf/configuration.properties).
  