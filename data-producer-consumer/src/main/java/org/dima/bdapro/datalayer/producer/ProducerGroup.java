package org.dima.bdapro.datalayer.producer;

import org.dima.bdapro.datalayer.consumer.ConsumerThread;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ProducerGroup {

    private final int numberOfProducers;
    private final String groupId;
    private final String topic;
    private final String brokers;
    private List<ProducerThread> producers;

    public ProducerGroup(String brokers, String groupId, String topic, int numberOfProducers) throws IOException {
        this.brokers = brokers;
        this.topic = topic;
        this.groupId = groupId;
        this.numberOfProducers = numberOfProducers;
        producers = new ArrayList<>();
        for (int i = 0; i < this.numberOfProducers; i++) {
            ProducerThread npThread =
                    new ProducerThread(this.brokers, this.groupId, this.topic, i);
            producers.add(npThread);
        }
    }

    public void execute() {
        for (ProducerThread ncThread : producers) {
            Thread t = new Thread(ncThread);
            t.start();
        }
    }

    /**
     * @return the numberOfConsumers
     */
    public int getNumberOfConsumers() {
        return numberOfProducers;
    }

    /**
     * @return the groupId
     */
    public String getGroupId() {
        return groupId;
    }

}
