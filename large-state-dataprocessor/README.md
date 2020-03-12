# bdapro-flink-large-state
This is the implementation of the business use case in Flink. 

## Java Documentation
Java documentation for the project can be found [here](javadoc)

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

Run the [producer](../data-producer-consumer/README.md).

### Configuration

Change the config in this [file](src/main/conf/flink-processor.properties).

Here are some important configurations:
* `flink.parallelism.source` determines the parallelism of the source operator of flink. Should be equal to the number of Kafka partitions.
* `flink.parallelism.default` determines the parallelism of all the other operators in the pipeline. Should be equal to the max number of task slots available in the cluster. i.e. number of task managers * number of task slots per task manager. 


Other configurations include Kafka related configurations.

### Build

````shell script
mvn clean package
````

### Run
#### IntelliJ
Select any of the Aggregation Job or Streaming Join Job classes and select run. 
We need to provide the command line arguments:

**AggregationStreamingJob.java**

* Reseller Usage Statistics
````
id /path/to/flink-processor.properties /path/to/output
````

* Level Usage Statistics
````
level /path/to/flink-processor.properties /path/to/output
````

**JoinStreamJob.java**
````
/path/to/flink-processor.properties /path/to/output
````

#### Cluster
Assuming the flink cluster is already up and running. Run the following for running the Job on flink.
**AggregationStreamingJob.java**

* Reseller Usage Statistics
````shell script
$FLINK_HOME/bin/flink/run -m "<flinkjobmanager:port>" -c org.dima.bdapro.flink.AggregationStreamingJob -d /path/to/large-state-dataprocessor-1.0-SNAPSHOT.jar id /path/to/flink-processor.properties /path/to/output
````

* Level Usage Statistics
````shell script
$FLINK_HOME/bin/flink/run -m "<flinkjobmanager:port>" -c org.dima.bdapro.flink.AggregationStreamingJob -d /path/to/large-state-dataprocessor-1.0-SNAPSHOT.jar level /path/to/flink-processor.properties /path/to/output
````

**JoinStreamJob.java**
````shell script
$FLINK_HOME/bin/flink/run -m "<flinkjobmanager:port>" -c org.dima.bdapro.flink.JoinStreamJob -d /path/to/large-state-dataprocessor-1.0-SNAPSHOT.jar /path/to/flink-processor.properties /path/to/output
````
