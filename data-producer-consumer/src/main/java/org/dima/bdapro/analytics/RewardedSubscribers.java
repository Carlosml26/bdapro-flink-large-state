package org.dima.bdapro.analytics;

import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.TransactionWrapper;
import org.dima.bdapro.jmx.Metrics;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.dima.bdapro.utils.Constants.SUBSCRIBER_TRANSACTION_PROFILE;
import static org.dima.bdapro.utils.Constants.TOPUP_PROFILE;

public class RewardedSubscribers extends AbstractReport {

	private ConcurrentHashMap<String, List<TransactionWrapper>> resellerTransactionMap = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, List<TransactionWrapper>> subscriberTransactionMap = new ConcurrentHashMap<>();

	private static RewardedSubscribers INSTANCE;

	private RewardedSubscribers() {
	}

	public static Report getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new RewardedSubscribers();

			Metrics metrics = new Metrics();
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			ObjectName rewardedUsageStatisticsName = null;

			try {
				System.out.println("entra");
				rewardedUsageStatisticsName = new ObjectName("com.rewardedUsageStatistics.metrics:type=rewardedUsageStatistics");
				mbs.registerMBean(metrics, rewardedUsageStatisticsName);
			} catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | MalformedObjectNameException e) {
				e.printStackTrace();
			}

			INSTANCE.setMetrics(metrics);
		}
		return INSTANCE;
	}

	public void init(String outputFileName, String statsFileName) throws IOException {
		initOutputFile(outputFileName);
		initStatsFile(statsFileName);
	}


	@Override
	public void processRecord(TransactionWrapper transactionW) {

		Transaction transaction = transactionW.getT();

		if (transaction.getProfileId().equals(TOPUP_PROFILE)) {
			String subscriberId = transaction.getReceiverId();
			List<TransactionWrapper> transactionQueue = resellerTransactionMap.get(subscriberId);
			if (transactionQueue == null) {
				synchronized (resellerTransactionMap) {
					transactionQueue = resellerTransactionMap.get(subscriberId);
					if (transactionQueue == null) { // double locking to for thread-safe initialization.

						transactionQueue = new ArrayList<>();
						resellerTransactionMap.put(subscriberId, transactionQueue);
					}
				}
			}
			transactionQueue.add(transactionW);
		}
		else if (transaction.getProfileId().equals(SUBSCRIBER_TRANSACTION_PROFILE)) {

			String subscriberId = transaction.getSenderId();
			List<TransactionWrapper> transactionQueue = subscriberTransactionMap.get(subscriberId);
			if (transactionQueue == null) {
				synchronized (subscriberTransactionMap) {
					transactionQueue = subscriberTransactionMap.get(subscriberId);
					if (transactionQueue == null) { // double locking to for thread-safe initialization.

						transactionQueue = new ArrayList<>();
						subscriberTransactionMap.put(subscriberId, transactionQueue);
					}
				}
			}
			transactionQueue.add(transactionW);
		}
	}

	@Override
	public void reset() {
		synchronized (resellerTransactionMap) {
			super.reset();
		}
	}

	@Override
	public void outputResults() throws IOException {
		String outputFormat = "%s, %d";
		Long timestamp;
		Long maxEventTime;
		Long maxProcTime;

		Long transactionSum;

		//Initialize metrics
		metrics.setTotalNumTransactions(0);
		eventTimeLatencySum = 0;
		processingTimeLatencySum= 0;


		synchronized (resellerTransactionMap) {
			maxEventTime = 0L;
			maxProcTime = 0L;



			for (Map.Entry<String, List<TransactionWrapper>> entry : resellerTransactionMap.entrySet()) {

				for (TransactionWrapper tw : entry.getValue()) {
					//statistics
					if (maxEventTime < tw.getEventTime()) {
						maxEventTime = tw.getEventTime();
					}

					if (maxProcTime < tw.getIngestionTime()) {
						maxProcTime = tw.getIngestionTime();
					}
				}

				timestamp = System.currentTimeMillis();

				//Prometheus
				long eventLatency = timestamp-maxEventTime;
				long procLatency = timestamp-maxProcTime;

				metrics.incTotalNumTransactions();

				eventTimeLatencySum += eventLatency;
				processingTimeLatencySum += procLatency;

				metrics.setEventTimeLatency(eventTimeLatencySum/metrics.getTotalNumTransactions());
				metrics.setProcessingTimeLatency(processingTimeLatencySum/metrics.getTotalNumTransactions());

				System.out.println(metrics.getEventTimeLatency());

				//logic
				if (isRewardedSubscriber(entry.getKey(), entry.getValue())) {
					outputFileWriter.append(String.format(outputFormat, entry.getKey(), maxEventTime));
					outputFileWriter.newLine();
				}

				statsFileWrtier.append(getStatsOutput(maxEventTime, maxProcTime, timestamp));
				statsFileWrtier.newLine();
			}
		}
	}

	private boolean isRewardedSubscriber(String key, List<TransactionWrapper> resellertransactions) {
		List<TransactionWrapper> subscribertransactions = subscriberTransactionMap.get(key);

		if (subscribertransactions != null) {
			Double resellerSum = resellertransactions.stream().map(x -> x.getT().getTransactionAmount())
					.reduce(Double::sum).orElse(0D);

			Double subscriberSum = subscribertransactions.stream().map(x -> x.getT().getTransactionAmount())
					.reduce(Double::sum).orElse(0D);

			return subscriberSum >= 0.4 * resellerSum;

		}
		return false;
	}

	private String getStatsOutput(Long maxEventTime, Long maxProcTime, Long timestamp) {
		String statsFormat = "%d, %d, %d"; // processing time latency, event time latency

		long eventLatency = timestamp - maxEventTime;
		long procLatency = timestamp - maxProcTime;


		return String.format(statsFormat,
				eventLatency,
				procLatency,
				maxEventTime);
	}
}