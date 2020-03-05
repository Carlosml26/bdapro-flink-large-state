package org.dima.bdapro.datalayer.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dima.bdapro.analytics.Report;
import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.TransactionWrapper;
import org.dima.bdapro.datalayer.bean.bytesarray.TransactionDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;



public class ConsumerThread implements Runnable {

	private static final Logger LOG = LogManager.getLogger(ConsumerThread.class);
	private final List<Report> reports;
	private KafkaConsumer<String, Transaction> internalConsumer;
	private String topic;
	private Properties props;
	private Object lock;
	private final int maxNumberProducers;
	private AtomicInteger numberProducers;

	public ConsumerThread(Properties props, Object lock, AtomicInteger numberProducers, List<Report> reports) throws IOException {
		internalConsumer = new KafkaConsumer<String, Transaction>(props, new StringDeserializer(), new TransactionDeserializer());
		topic = props.getProperty("topic");
		this.props = props;
		internalConsumer.subscribe(Arrays.asList(topic));
		this.lock = lock;
		this.numberProducers = numberProducers;
		maxNumberProducers = Integer.parseInt(props.getProperty("n_consumers"));
		this.reports = reports;
	}

	@Override
	public void run() {
		try {

			LOG.debug("Thread started: {}",Thread.currentThread().getName());

			Long currentTime;
			Long windowsSize = Long.parseLong(props.getProperty("dataconsumer.query.time_window_size_ms"));
			Long windowsStart = 0L;
			Long windowEnd = windowsStart + windowsSize;

			while (true) {
				ConsumerRecords<String, Transaction> records = internalConsumer.poll(Duration.ofMillis(Long.parseLong(props.getProperty("dataconsumer.kafka.polling-time"))));
				LOG.debug("Returned After polling .... Returned records: {} , subscription: {}",records.count(), internalConsumer.subscription());

				for (ConsumerRecord<String, Transaction> record : records) {

					Transaction transaction = record.value();
					currentTime = transaction.getTransactionTime() - 1;
					long ingestionTime = System.currentTimeMillis();

					if (currentTime > windowEnd) {

						synchronized (lock) {
							LOG.debug("Materializing Windows ....");
							numberProducers.decrementAndGet();
							if (numberProducers.get() == 0) {

								processReports();

								numberProducers.set(maxNumberProducers);
//								System.out.println(Thread.currentThread().getName() + ": WakeUpALL!");

								lock.notifyAll();
							}
							else {
								LOG.debug("Thread going to sleep: {}",Thread.currentThread().getName());
//								System.out.println(Thread.currentThread().getName() + ": sleeping");
								lock.wait(2000); // TODO: All threads go to sleep, somehow.
							}
						}

						long x = windowEnd;
						windowEnd = windowsStart == 0 ? currentTime + windowsSize : windowsStart + windowsSize;
						windowsStart = windowsStart == 0 ? currentTime : x;

					}

					addRecordToRespectiveQueues(new TransactionWrapper(transaction, ingestionTime, currentTime));
				}
			}

		}
		catch (InterruptedException e) {
			LOG.error("Normal Execution of Consumer Thread {} has been interrupted.", Thread.currentThread().getName(), e);
			throw new RuntimeException(e);
		}
		catch (WakeupException e) {
			LOG.error("Normal Execution of Consumer Thread {} has been woke up externally", Thread.currentThread().getName(), e);
		}
		finally {
			internalConsumer.close();
			LOG.info("Closing consumer {}", Thread.currentThread().getName());
		}

	}

	private void addRecordToRespectiveQueues(TransactionWrapper transaction) {
		reports.forEach(x -> x.process(transaction));
	}

	private void processReports() {
		for (Report report : reports) {
			try {
				report.materialize();
			}
			catch (IOException e) {
				LOG.error(e);
			}
		}
	}

	public KafkaConsumer<String, Transaction> getInternalConsumer() {
		return internalConsumer;
	}
}
