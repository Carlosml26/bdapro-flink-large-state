package org.dima.bdapro.datalayer.consumer;

import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class ConsumerGroup {

	private final int numberOfConsumers;
	private final String topic;
	private List<ConsumerThread> consumers;

	public ConsumerGroup() throws IOException {
		Properties properties = PropertiesHandler.getInstance().getModuleProperties();
		this.numberOfConsumers = Integer.parseInt(properties.getProperty("n_consumers"));
		this.topic = properties.getProperty("topic");

		consumers = new ArrayList<>();
		for (int i = 0; i < this.numberOfConsumers; i++) {
			ConsumerThread ncThread =
					new ConsumerThread(this.topic);
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
