package org.dima.bdapro.analytics;

import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.TransactionWrapper;
import org.dima.bdapro.utils.TransactionMedianCalculator;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.dima.bdapro.utils.Constants.RESELLER_TRANSACTION_PROFILE;
import static org.dima.bdapro.utils.Constants.TOPUP_PROFILE;

public class ResellerUsageStatistics extends AbstractReport implements Report {

	private ConcurrentHashMap<String, TransactionMedianCalculator> transactionMap = new ConcurrentHashMap<>();

	private static ResellerUsageStatistics INSTANCE;

	private ResellerUsageStatistics() {
	}

	public static Report getInstance() {
		if (INSTANCE == null) {

			INSTANCE = new ResellerUsageStatistics();

			Metrics metrics = new Metrics();
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			ObjectName resellerUsageStatisticsName = null;

			try {
				resellerUsageStatisticsName = new ObjectName("com.resellerUsageStatistics.metrics:type=resellerUsageStatistics");
				mbs.registerMBean(metrics, resellerUsageStatisticsName);
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

		String transactionSenderId = transaction.getSenderId();

		if (transaction.getProfileId().equals(RESELLER_TRANSACTION_PROFILE) || transaction.getProfileId().equals(TOPUP_PROFILE)) {

			TransactionMedianCalculator transactionQueue = transactionMap.get(transactionSenderId);
			if (transactionQueue == null) {
				synchronized (transactionMap) {
					transactionQueue = transactionMap.get(transactionSenderId);
					if (transactionQueue == null) { // double locking to for thread-safe initialization.

						transactionQueue = new TransactionMedianCalculator();
						transactionMap.put(transactionSenderId, transactionQueue);
					}
				}
			}
			transactionQueue.add(transactionW);
		}
	}

	@Override
	public void reset() {
		synchronized (transactionMap) {
			super.reset();
			for (TransactionMedianCalculator e : transactionMap.values()) {
				e.reset();
			}
		}
	}

	@Override
	public void outputResults() throws IOException {
		String outputFormat = "%s, %d,%.2f";
		Long timestamp;



		synchronized (transactionMap) {
			for (Map.Entry<String, TransactionMedianCalculator> entry : transactionMap.entrySet()) {
				TransactionWrapper wrapper = entry.getValue().median();
				if (wrapper == null) {
					continue;
				}

				timestamp = System.currentTimeMillis();

				//Set metrics
				double eventLatency = timestamp-wrapper.getEventTime();
				double procLatency = timestamp-wrapper.getIngestionTime();

				metrics.incTotalNumTransactions();

				eventTimeLatencySum += eventLatency;
				processingTimeLatencySum += procLatency;

				metrics.setEventTimeLatency(eventTimeLatencySum/metrics.getTotalNumTransactions());
				metrics.setProcessingTimeLatency(processingTimeLatencySum/metrics.getTotalNumTransactions());


				outputFileWriter.append(String.format(outputFormat, entry.getKey(), wrapper.getEventTime(), wrapper.getT().getTransactionAmount()));
				outputFileWriter.newLine();

				statsFileWrtier.append(getStatsOutput(wrapper, timestamp));
				statsFileWrtier.newLine();
			}
		}
	}

	private String getStatsOutput(TransactionWrapper wrapper, Long timestamp) {
		String statsFormat = "%d, %d, %d"; // processing time latency, event time latency

		long eventLatency = timestamp - wrapper.getEventTime();
		long procLatency = timestamp - wrapper.getIngestionTime();


		return String.format(statsFormat,
				eventLatency,
				procLatency, wrapper.getEventTime());
	}
}
