package org.dima.bdapro.datalayer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.json.TransactionDeserializer;

import java.util.Arrays;
import java.util.Properties;


public class ConsumerThread implements Runnable {

    private final KafkaConsumer<String, Transaction> consumer;
    private final String topic;

    public ConsumerThread(String brokers, String groupId, String topic) {
        Properties prop = createConsumerConfig(brokers, groupId);
        this.consumer = new KafkaConsumer<String, Transaction>(prop,new StringDeserializer (), new TransactionDeserializer<Transaction>(Transaction.class));
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, Transaction> records = consumer.poll(100);
            for (ConsumerRecord<String, Transaction> record : records) {
                System.out.println("Receive transaction: " + record.value().getTransactionId() + " from "
                        + record.value().getSenderId() + " to " + record.value().getReceiverId() + ", Partition: "
                        + record.partition() + ", Offset: " + record.offset() + ", by ThreadID: "
                        + Thread.currentThread().getId());
            }
        }

    }

}
