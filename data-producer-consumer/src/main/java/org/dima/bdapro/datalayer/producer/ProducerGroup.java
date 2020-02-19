package org.dima.bdapro.datalayer.producer;

import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class ProducerGroup {

	private final int numberOfProducers;
	private int maxMessagesPerSecond;
	private List<ProducerThread> producers;

	public ProducerGroup() throws IOException {
		this.numberOfProducers = Integer.parseInt(PropertiesHandler.getInstance().getModuleProperties().getProperty("n_producers"));
		producers = new ArrayList<>();
		this.maxMessagesPerSecond = Integer.parseInt(PropertiesHandler.getInstance().getModuleProperties().getProperty("max_number_messages"));

		int maxMessagesPerThread = Math.floorDiv(maxMessagesPerSecond, numberOfProducers);
		int remainder = maxMessagesPerThread%numberOfProducers;

		for (int i = 0; i < this.numberOfProducers; i++) {
			ProducerThread npThread;
			if (i < remainder)
				npThread = new ProducerThread(i,maxMessagesPerThread+1);
			else
				npThread = new ProducerThread(i,maxMessagesPerThread);

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
