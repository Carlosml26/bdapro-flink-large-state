#cd $APACHE_KAFKA_HOME
 /mnt/fast/SApplications/kafka_2.13-2.4.0/bin/kafka-topics.sh --create --zookeeper localhost:2181  --replication-factor 1 --partitions 3 --topic HelloKafkaTopic1