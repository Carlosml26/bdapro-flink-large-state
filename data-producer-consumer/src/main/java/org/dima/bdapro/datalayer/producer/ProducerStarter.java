package org.dima.bdapro.datalayer.producer;

import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;

public final class ProducerStarter {

	public static void main(String[] args) throws IOException {


		PropertiesHandler.getInstance(args != null && args.length > 1 ? args[0] : "./data-producer-consumer/src/main/conf/configuration.properties");


		ProducerGroup producerGroup = new ProducerGroup();

		producerGroup.execute();

		try {
			Thread.sleep(100000);
		}
		catch (InterruptedException ie) {

		}
	}
}

