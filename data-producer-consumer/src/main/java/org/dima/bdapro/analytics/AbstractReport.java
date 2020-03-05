package org.dima.bdapro.analytics;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dima.bdapro.datalayer.bean.TransactionWrapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public abstract class AbstractReport implements Report {

	private static final Logger LOG = LogManager.getLogger(AbstractReport.class);

	protected BufferedWriter outputFileWriter;
	protected BufferedWriter statsFileWrtier;

	protected Metrics metrics;

	protected double eventTimeSum = 0;
	protected double processingTimeSum = 0;
	protected final Gauge processingTimeLatencyGauge = Gauge.build().name("ProcessingLatencyGauge").help("Inprogress requests.").register();
	protected final Gauge eventTimeLatencyGauge = Gauge.build().name("EventLatencyGauge").help("Inprogress requests.").register();
	protected static final Counter numberEventCount = Counter.build().name("reventCounter").help("Total requests.").register();


	protected double eventTimeLatencySum = 0;
	protected double processingTimeLatencySum = 0;



	@Override
	public void process(TransactionWrapper wrapper) {
		processRecord(wrapper);
		LOG.debug("Record Added: {}", wrapper);
	}

	@Override
	public void materialize() throws IOException {
		outputResults();
		reset();
	}

	public void reset() {
		try {
			outputFileWriter.flush();
			statsFileWrtier.flush();
		}
		catch (IOException e) {
			LOG.error(e);
		}
	}

	protected void initStatsFile(String statsFileName) throws IOException {
		statsFileWrtier = createWriter(statsFileName);
	}

	protected void initOutputFile(String outputFileName) throws IOException {
		outputFileWriter = createWriter(outputFileName);
	}

	private BufferedWriter createWriter(String fileName) throws IOException {
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
			file.createNewFile();
		}
		return new BufferedWriter(new FileWriter(file));
	}

	@Override
	public void close() throws IOException {
		outputFileWriter.close();
		statsFileWrtier.close();
	}

	public void setMetrics (Metrics metrics){
		this.metrics =  metrics;
	}

}
