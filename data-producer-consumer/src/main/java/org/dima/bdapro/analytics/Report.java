package org.dima.bdapro.analytics;

import org.dima.bdapro.datalayer.bean.Transaction;

public interface Report {
	void processRecord(Transaction transaction);
}
