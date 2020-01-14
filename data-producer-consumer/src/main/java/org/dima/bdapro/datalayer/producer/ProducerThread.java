package org.dima.bdapro.datalayer.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.json.TransactionSerializer;

import java.util.Properties;

public class ProducerThread implements Runnable {

    private final KafkaProducer<String, Transaction> producer;
    private final String topic;
    private final Integer producerNumber;


    public ProducerThread(String brokers, String groupId, String topic, int producerNumber) {
        Properties prop = createProducerConfig(brokers,groupId);
        this.producer = new KafkaProducer<String, Transaction>(prop, new StringSerializer(), new TransactionSerializer());
        this.topic = topic;
        this.producerNumber = producerNumber;
    }

    private static Properties createProducerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    @Override
    public void run() {
        System.out.println("Produces 3 messages");
        for (int i = 0; i < 5; i++) {
            Transaction msg = new Transaction(null,"Transaction"+i,"Sender"+producerNumber,"SenderType"+producerNumber,"Receiver"+i,"ReceiverType"+i, 50.0);

            producer.send(new ProducerRecord<String, Transaction>(topic, producerNumber.toString(), msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    System.out.println("Sent transaction " + msg.getTransactionId() + " from "
                            + msg.getSenderId() + " to " + msg.getReceiverId()
                            + ", Partition: " + metadata.partition() + ", Offset: "
                            + metadata.offset());
                }
            });

        }
        // closes producer
        producer.close();

    }


}
