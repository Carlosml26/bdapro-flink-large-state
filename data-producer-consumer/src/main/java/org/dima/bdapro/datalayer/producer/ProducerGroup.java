package org.dima.bdapro.datalayer.producer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ProducerGroup {

	private static final Logger LOG = LogManager.getLogger(ProducerGroup.class);

	private List<ProducerThread> producers;

	public ProducerGroup() throws IOException {
		int numberOfProducers = Integer.parseInt(PropertiesHandler.getInstance().getModuleProperties().getProperty("n_producers"));
		producers = new ArrayList<>(numberOfProducers);


		int maxMessagesPerSecond = Integer.parseInt(PropertiesHandler.getInstance().getModuleProperties().getProperty("max_rate_messages"));
		int maxNumberOfMessages = Integer.parseInt(PropertiesHandler.getInstance().getModuleProperties().getProperty("max_number_messages"));

		int maxRatePerThread = Math.floorDiv(maxMessagesPerSecond, numberOfProducers);
		int remMps = maxRatePerThread % numberOfProducers;

		int maxMessagesPerThread = Math.floorDiv(maxNumberOfMessages, numberOfProducers);
		int remMpThread = maxMessagesPerThread % numberOfProducers;

		for (int id = 0; id < numberOfProducers; id++) {
			ProducerThread npThread;
			int rate = id < remMps ? maxRatePerThread + 1 : maxRatePerThread;
			int numMessages = id < remMpThread ? maxMessagesPerThread + 1 : maxMessagesPerThread;

			npThread = new ProducerThread(id, rate, numMessages);

			producers.add(npThread);
		}


		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				LOG.info("Shutdown hook invoked...");

				for (int i = 0; i < producers.size(); i++) {
					ProducerThread producerThread = producers.get(i);
					producerThread.getInternalProducer().flush();
					producerThread.getInternalProducer().close();
				}
			}
		});

		try {
			Thread.sleep(500);
		}catch (InterruptedException e) {
			LOG.debug("{} interrupted", Thread.currentThread().getName());
		}
	}

	public void execute() {
		for (int i = 0; i < producers.size(); i++) {
			ProducerThread ncThread = producers.get(i);
			Thread t = new Thread(ncThread);
			t.start();

		}
	}
}
