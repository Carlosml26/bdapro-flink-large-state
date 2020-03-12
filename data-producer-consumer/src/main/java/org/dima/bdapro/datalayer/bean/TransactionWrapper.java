package org.dima.bdapro.datalayer.bean;

/**
 * A wrapper for {@link Transaction} that adds ingestion time and event time.
 */
public class TransactionWrapper {

	private Transaction t;
	private long ingestionTime;
	private long eventTime;

	public TransactionWrapper(Transaction t, long ingestionTime, long eventTime) {
		this.t = t;
		this.ingestionTime = ingestionTime;
		this.eventTime = eventTime;
	}

	public Transaction getT() {
		return t;
	}

	public void setT(Transaction t) {
		this.t = t;
	}

	public long getIngestionTime() {
		return ingestionTime;
	}

	public void setIngestionTime(long ingestionTime) {
		this.ingestionTime = ingestionTime;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	@Override
	public String toString() {
		return "TransactionWrapper{" +
				"t=" + t +
				", ingestionTime=" + ingestionTime +
				", eventTime=" + eventTime +
				'}';
	}
}
