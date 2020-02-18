package org.dima.bdapro.datalayer.consumer;



import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public final class ConsumerGroup {

    private List<ConsumerThread> consumers;
    private final AtomicInteger numberOfConsumers;
    private final String topic;
    private ConcurrentHashMap<String, PriorityBlockingQueue<Transaction>> transactionMap = new ConcurrentHashMap<>();
    private Object lock = new Object();


    public ConsumerGroup(String propsPath) throws IOException {
        Properties properties = PropertiesHandler.getInstance(propsPath != null  ? propsPath : "../large-state-dataprocessor/src/main/conf/java-processor.properties").getModuleProperties();
        this.numberOfConsumers = new AtomicInteger(Integer.parseInt(properties.getProperty("n_consumers")));
        this.topic = properties.getProperty("topic");
        consumers = new ArrayList<>();

        for (int i = 0; i < numberOfConsumers.get(); i++) {
            ConsumerThread ncThread = new ConsumerThread(transactionMap,properties,lock, numberOfConsumers);
            consumers.add(ncThread);
        }
    }

    public void execute() {
        for (ConsumerThread ncThread : consumers) {
            Thread t = new Thread(ncThread);
            t.start();
        }
    }
}
