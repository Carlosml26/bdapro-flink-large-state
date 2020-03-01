package org.dima.bdapro.datalayer.consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;

public final class ConsumerStarter {

    private static final Logger LOG = LogManager.getLogger(ConsumerStarter.class);

    public static void main(String[] args) throws IOException {


        PropertiesHandler.getInstance(args != null && args.length >= 1 ? args[0] : "data-producer-consumer/src/main/conf/configuration.properties");

        // Start group of Notification Consumers
        ConsumerGroup consumerGroup = new ConsumerGroup(null);

        LOG.info("Consumer Started...");
        consumerGroup.execute();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
    }
}
