package org.dima.bdapro.datalayer.consumer;

public final class ConsumerStarter {

    public static void main(String[] args) {

        String brokers = "localhost:9092";
        String groupId = "group01";
        String topic = "HelloKafkaTopic1";
        int numberOfConsumer = 3;


        if (args != null && args.length > 4) {
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
            numberOfConsumer = Integer.parseInt(args[3]);
        }

        // Start group of Notification Consumers
        ConsumerGroup consumerGroup =
                new ConsumerGroup(brokers, groupId, topic, numberOfConsumer);

        consumerGroup.execute();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
    }
}
