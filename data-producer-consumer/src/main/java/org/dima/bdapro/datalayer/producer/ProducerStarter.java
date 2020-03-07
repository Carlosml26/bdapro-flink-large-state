package org.dima.bdapro.datalayer.producer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;

public final class ProducerStarter {

	private static final Logger LOG = LogManager.getLogger(ProducerStarter.class);
	public static void main(String[] args) throws IOException {

		PropertiesHandler.getInstance(args != null && args.length >= 1 ? args[0] : "data-producer-consumer/src/main/conf/configuration.properties");


		ProducerGroup producerGroup = new ProducerGroup();

		LOG.info("Producer started...");
		producerGroup.execute();

		try {
			Thread.sleep(1000);
		}
		catch (InterruptedException ie) {

		}
	}
}

