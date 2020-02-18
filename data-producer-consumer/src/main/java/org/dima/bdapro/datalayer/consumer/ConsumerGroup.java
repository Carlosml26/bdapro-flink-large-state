package org.dima.bdapro.datalayer.consumer;


import org.dima.bdapro.analytics.Report;
import org.dima.bdapro.analytics.LevelUsageStatistics;
import org.dima.bdapro.analytics.ResellerUsageStatistics;
import org.dima.bdapro.analytics.RewardedSubscribers;
import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public final class ConsumerGroup {

	private List<ConsumerThread> consumers;
	private final AtomicInteger numberOfConsumers;
	private final String topic;
	private ConcurrentHashMap<String, PriorityBlockingQueue<Transaction>> transactionMap = new ConcurrentHashMap<>();
	private Object lock = new Object();


	public ConsumerGroup(String propsPath) throws IOException {
		Properties properties = PropertiesHandler.getInstance(propsPath != null ? propsPath : "../large-state-dataprocessor/src/main/conf/java-processor.properties").getModuleProperties();
		this.numberOfConsumers = new AtomicInteger(Integer.parseInt(properties.getProperty("n_consumers")));
		this.topic = properties.getProperty("topic");
		consumers = new ArrayList<>();

		List<Report> reports = new ArrayList<>(3);
		if ("ResellerUsageStatistics".equals(properties.getProperty("dataconsumer.query"))) {
			reports.add(ResellerUsageStatistics.getInstance());
			ResellerUsageStatistics.getInstance().init("java-reseller-output.txt", "java-reseller-stats.txt");
		}
		else if ("LevelUsageStatistics".equals(properties.getProperty("dataconsumer.query"))) {
			reports.add(LevelUsageStatistics.getInstance());
			LevelUsageStatistics.getInstance().init("java-level-output.txt", "java-level-stats.txt");
		}
		else if ("RewardedSubscribers".equals(properties.getProperty("dataconsumer.query"))) {
			reports.add(RewardedSubscribers.getInstance());
			RewardedSubscribers.getInstance().init("java-subscribers-output.txt", "java-subscribers-stats.txt");
		}
		else if ("ALL".equals(properties.getProperty("dataconsumer.query"))) {
			reports.add(ResellerUsageStatistics.getInstance());
			ResellerUsageStatistics.getInstance().init("java-reseller-output.txt", "java-reseller-stats.txt");

			reports.add(LevelUsageStatistics.getInstance());
			LevelUsageStatistics.getInstance().init("java-level-output.txt", "java-level-stats.txt");

			reports.add(RewardedSubscribers.getInstance());
			RewardedSubscribers.getInstance().init("java-subscribers-output.txt", "java-subscribers-stats.txt");

		}


		for (int i = 0; i < numberOfConsumers.get(); i++) {
			ConsumerThread ncThread = new ConsumerThread(transactionMap, properties, lock, numberOfConsumers, reports);
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
