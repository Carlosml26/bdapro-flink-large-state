# Data Producer and Single Node Java Producer
## Documentation
Java documentation for the project can be found [here](javadoc)

Monitoring tools used are Prometheus and Grafana. Corresponding prometheus configuration and Grafana dashboard can be found [here](kafka-tools).

## Setup instructions
Setup KAFKA_HOME environment variable and N_PARTITIONS for number of kafka partitions in [environment.sh](src/main/scripts/environment.sh) script.

```shell script
export KAFKA_HOME="/path/to/kafka_xx_xx"
export N_PARTITONS=16
```

Run the following three scripts for starting zookeeper, kafka and creating the topic on kafka.

```shell script
scripts/start-zookeeper.sh
scripts/start-kafka.sh
scripts/create-topics.sh
```

## Data Generator
### Configuration

Change the config in this [file](src/main/conf/configuration.properties).

Here are some important configurations:
* `n_producers` determines the number of Java threads to run for producing the data.
* `datagenerator.resellers.*` determines the characteristics (i.e. reseller types and cardinality) of the data being produced with respect to the stake holders (resellers, subscribers).
* `datagenerator.transaction.*` determines the proportion of the transaction types being produced.

Other configurations include Kafka related configurations.

### Build

````shell script
mvn clean package
````

### Run

```shell script
java -Dlog4j.configurationFile=</path/to/log4j2.xml> -Xms4g -Xmx8g -cp </path/to/kafka-multithreaded-producer-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar> org.dima.bdapro.datalayer.producer.ProducerStarter </path/to/configuration.properties>
```


## Single Node Java Consumer
### Configuration

Change the config in this [file](src/main/conf/configuration.properties).

Here are some important configurations:
* `n_consumers` determines the number of Java threads to run for consuming the data.
* `dataconsumer.query.*` determines which query to run and what should be the size of the tumbling window.

Other configurations include Kafka related configurations.

### Build

````shell script
mvn clean package
````

### Run

```shell script
java -Dlog4j.configurationFile=</path/to/log4j2.xml> -Xms4g -Xmx8g -cp </path/to/kafka-multithreaded-producer-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar> org.dima.bdapro.datalayer.consumer.ConsumerStarter </path/to/configuration.properties>
```