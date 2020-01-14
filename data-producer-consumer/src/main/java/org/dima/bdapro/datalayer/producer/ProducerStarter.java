package org.dima.bdapro.datalayer.producer;

public final class ProducerStarter {

public static void main(String[] args) {

        String brokers = "localhost:9092";
        String groupId = "group01";
        String topic = "HelloKafkaTopic1";
        int numberOfProducer = 3;


        if (args != null && args.length > 4) {
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
            numberOfProducer = Integer.parseInt(args[3]);
        }


        ProducerGroup producerGroup = new ProducerGroup(brokers, groupId, topic, numberOfProducer);

        producerGroup.execute();

        try {
        Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
        }
}

