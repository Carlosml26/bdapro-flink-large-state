package org.dima.bdapro.datalayer.consumer;

import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;

public final class ConsumerStarter {

    public static void main(String[] args) throws IOException {


        PropertiesHandler.getInstance(args != null && args.length > 1 ? args[0] : "src/main/conf/configuration.properties");


        // Start group of Notification Consumers
        ConsumerGroup consumerGroup =
                new ConsumerGroup();

        consumerGroup.execute();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
    }
}
