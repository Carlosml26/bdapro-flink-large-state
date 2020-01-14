package org.dima.bdapro.datalayer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.json.TransactionDeserializer;
import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerThread implements Runnable {

    private final KafkaConsumer<String, Transaction> consumer;
    private final String topic;

    public ConsumerThread(String topic) throws IOException {
        Properties prop = PropertiesHandler.getInstance().getModuleProperties();
        this.consumer = new KafkaConsumer<String, Transaction>(prop,new StringDeserializer (), new TransactionDeserializer<Transaction>(Transaction.class));
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
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
