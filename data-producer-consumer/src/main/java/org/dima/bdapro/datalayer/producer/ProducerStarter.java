package org.dima.bdapro.datalayer.producer;

import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;

public final class ProducerStarter {

	public static void main(String[] args) throws IOException {

		String brokers = "localhost:9092";
		String groupId = "group01";
		String topic = "HelloKafkaTopic1";
		int numberOfProducer = 3;


		if (args != null && args.length > 5) {
			brokers = args[0];
			groupId = args[1];
			topic = args[2];
			numberOfProducer = Integer.parseInt(args[3]);
            PropertiesHandler.getInstance(args[4]);
		}

		PropertiesHandler.getInstance("src/main/conf/configuration.properties");


		ProducerGroup producerGroup = new ProducerGroup(brokers, groupId, topic, numberOfProducer);

		producerGroup.execute();

		try {
			Thread.sleep(100000);
		}
		catch (InterruptedException ie) {

		}
	}
}

