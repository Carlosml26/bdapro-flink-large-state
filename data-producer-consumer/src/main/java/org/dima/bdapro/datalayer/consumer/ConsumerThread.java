package org.dima.bdapro.datalayer.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.json.TransactionDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.dima.bdapro.utils.Constants.RESELLER_TRANSACTION_PROFILE;
import static org.dima.bdapro.utils.Constants.TOPUP_PROFILE;


public class ConsumerThread implements Runnable {

	private ConcurrentHashMap<String, PriorityBlockingQueue<Transaction>> transactionHasMap;
	private KafkaConsumer<String, Transaction> consumer;
	private String topic;
	private Properties props;
	private Object lock;
	private final int maxNumberProducers;
	private AtomicInteger numberProducers;

	public ConsumerThread(ConcurrentHashMap<String, PriorityBlockingQueue<Transaction>> transactionHasMap, Properties props, Object lock, AtomicInteger numberProducers) throws IOException {
		consumer = new KafkaConsumer<String, Transaction>(props, new StringDeserializer(), new TransactionDeserializer<Transaction>(Transaction.class));
		topic = props.getProperty("topic");
		this.props = props;
		consumer.subscribe(Arrays.asList(topic));
		this.transactionHasMap = transactionHasMap;
		this.lock = lock;
		this.numberProducers = numberProducers;
		maxNumberProducers = Integer.parseInt(props.getProperty("n_consumers"));
	}


	class TransactionComparator implements Comparator<Transaction> {
		@Override
		public int compare(Transaction t1, Transaction t2) {
			if (t1.getTransactionAmount() < t2.getTransactionAmount()) {
				return 1;
			}
			if (t1.getTransactionAmount() > t2.getTransactionAmount()) {
				return -1;
			}
			return 0;
		}
	}

	public void outPut() {

	}

	@Override
	public void run() {
		try {

			Long currentTime;
			Long windowsSize = 20000L; // Long.parseLong(props.getProperty("java.query.agg_per_ressellerId.time_window_size_ms"));
			Long windowsStart = 0L;
			Long windowEnd = windowsStart + windowsSize;

			while (true) {
				ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(200));

				int zzz = 0;
				for (ConsumerRecord<String, Transaction> record : records) {

					Transaction transaction = record.value();
					String transactionSenderId = transaction.getSenderId();
					String transactionSenderType = transaction.getSenderType();
					currentTime = transaction.getTransactionTime() - 1;

					if (currentTime > windowEnd) {

						synchronized (lock) {
							numberProducers.decrementAndGet();
							if (numberProducers.get() == 0) {


								for (PriorityBlockingQueue<Transaction> transactionsQueue : transactionHasMap.values()) {
									Transaction t = getMediandTransaction(transactionsQueue);
									outPut(); //TODO
									transactionsQueue = null;
								}
								transactionHasMap = new ConcurrentHashMap<>();
								numberProducers.set(maxNumberProducers);
								System.out.println(Thread.currentThread().getName() + ": WakeUpALL!");

								lock.notifyAll();
							}

//							while (numberProducers.get() >= 0) {
								System.out.println(Thread.currentThread().getName() + ": sleeping");
								lock.wait(windowsSize); // TODO: All threads go to sleep, somehow.
//							}
						}

						long x = windowEnd;
						windowEnd = windowsStart == 0 ? currentTime + windowsSize : windowsStart + windowsSize;
						windowsStart = windowsStart == 0 ? currentTime : x;

					}

					if (transaction.getProfileId().equals(RESELLER_TRANSACTION_PROFILE) || transaction.getProfileId().equals(TOPUP_PROFILE)) {

						PriorityBlockingQueue<Transaction> transactionQueue = transactionHasMap.get(transactionSenderId);
						if (transactionQueue == null) {
							PriorityBlockingQueue<Transaction> transactions = new PriorityBlockingQueue<>(100, new TransactionComparator());
							transactions.add(transaction);
							transactionHasMap.put(transactionSenderId, transactions);
//							System.out.println("New queue created  " + transactionSenderId + " and transaction " + transaction.getTransactionId() + " introduced");
						}
						else {
//							System.out.println("Transaction " + transaction.getTransactionId() + " in the queue " + transactionSenderId);
							transactionQueue.add(transaction);
						}
					}
				}
			}

		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}

	}


	private Transaction getMediandTransaction(PriorityBlockingQueue<Transaction> transactionsQueue) {
		return null;
	}
}
