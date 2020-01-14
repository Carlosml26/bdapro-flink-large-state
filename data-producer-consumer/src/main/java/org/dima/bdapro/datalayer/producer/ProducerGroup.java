package org.dima.bdapro.datalayer.producer;

import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ProducerGroup {

	private final int numberOfProducers;
	private List<ProducerThread> producers;

	public ProducerGroup() throws IOException {
		this.numberOfProducers = Integer.parseInt(PropertiesHandler.getInstance().getModuleProperties().getProperty("n_producers"));
		producers = new ArrayList<>();
		for (int i = 0; i < this.numberOfProducers; i++) {
			ProducerThread npThread = new ProducerThread(i);
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
