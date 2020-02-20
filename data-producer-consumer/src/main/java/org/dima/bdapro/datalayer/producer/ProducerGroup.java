package org.dima.bdapro.datalayer.producer;

import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ProducerGroup {

	private List<ProducerThread> producers;

	public ProducerGroup() throws IOException {
		int numberOfProducers = Integer.parseInt(PropertiesHandler.getInstance().getModuleProperties().getProperty("n_producers"));
		producers = new ArrayList<>();

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
	}

	public void execute() {
		for (ProducerThread ncThread : producers) {
			Thread t = new Thread(ncThread);
			t.start();
		}
	}
}
