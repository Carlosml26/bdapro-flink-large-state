package org.dima.bdapro.analytics;

import org.dima.bdapro.datalayer.bean.TransactionWrapper;
import org.dima.bdapro.jmx.Metrics;

import java.io.IOException;

public interface Report {
	void process(TransactionWrapper transaction);
	void processRecord(TransactionWrapper transaction);
	void materialize() throws IOException;
	void outputResults() throws IOException;
	void init(String outputFileName, String statsFileName) throws IOException;
	void close() throws IOException;
	Metrics getMetrics();
}


