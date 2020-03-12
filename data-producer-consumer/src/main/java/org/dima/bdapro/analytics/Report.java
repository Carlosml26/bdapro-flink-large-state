package org.dima.bdapro.analytics;

import org.dima.bdapro.datalayer.bean.TransactionWrapper;
import org.dima.bdapro.jmx.Metrics;

import java.io.IOException;

/**
 * Provides an interface for interacting with the report queries.
 */
public interface Report {
	/**
	 * Add new record to the window for procesing.
	 * @param transaction
	 */
	void process(TransactionWrapper transaction);

	/**
	 * Finalize and materialize the windows.
	 * @throws IOException
	 */
	void materialize() throws IOException;

	/**
	 * Initialize the query and open files for writing.
	 * @param outputFileName
	 * @param statsFileName
	 * @throws IOException
	 */
	void init(String outputFileName, String statsFileName) throws IOException;

	/**
	 * Close all the output files.
	 * @throws IOException
	 */
	void close() throws IOException;
	Metrics getMetrics();
}


