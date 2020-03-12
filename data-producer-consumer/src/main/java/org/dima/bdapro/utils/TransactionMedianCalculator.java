package org.dima.bdapro.utils;

import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.TransactionWrapper;

import java.util.function.BiFunction;

/**
 * An online way of calculating median. Implementation is inspired from:
 * https://stackoverflow.com/questions/11955728/how-to-calculate-the-median-of-an-array/59905644#59905644
 *
 * This implementation is tuned for out {@link TransactionWrapper}
 */
public class TransactionMedianCalculator {

	private BiFunction<TransactionWrapper, TransactionWrapper, Integer> comparator;
	private BiFunction<TransactionWrapper, TransactionWrapper, TransactionWrapper> average;


	private TransactionWrapper maxOfLowerHalf;
	private TransactionWrapper lowestOfUpperHalf;

	private int lowerHalfCount;
	private int upperHalfCount;

	private long maxProcTime;
	private long maxEventTime;


	public TransactionMedianCalculator() {

		comparator = new BiFunction<TransactionWrapper, TransactionWrapper, Integer>() {
			@Override
			public Integer apply(TransactionWrapper wrapper, TransactionWrapper wrapper2) {
				return apply(wrapper.getT(), wrapper2.getT());
			}

			private Integer apply(Transaction transaction, Transaction transaction2) {
				return transaction.getTransactionAmount().compareTo(transaction2.getTransactionAmount());
			}
		};

		average = (wrapper, wrapper2) -> {
			wrapper2.getT().setTransactionAmount((wrapper.getT().getTransactionAmount() + wrapper2.getT().getTransactionAmount()) / 2.0);
			return wrapper2;
		};
	}

	public synchronized void add(TransactionWrapper e) {
		if (lowerHalfCount <= upperHalfCount) {

			if (maxOfLowerHalf == null) {
				maxOfLowerHalf = e;
			}
			else if (comparator.apply(maxOfLowerHalf, e) < 0) { // pick large one
				maxOfLowerHalf = e;
			}
			lowerHalfCount++;
		}
		else {
			if (lowestOfUpperHalf == null) {
				lowestOfUpperHalf = e;
			}
			else if (comparator.apply(lowestOfUpperHalf, e) > 0) { // pick small one
				lowestOfUpperHalf = e;
			}
			upperHalfCount++;
		}

		//balance
		if (lowestOfUpperHalf != null && comparator.apply(maxOfLowerHalf, lowestOfUpperHalf) > 0) {
			TransactionWrapper x = maxOfLowerHalf;
			maxOfLowerHalf = lowestOfUpperHalf;
			lowestOfUpperHalf = x;
		}


		if (maxEventTime < e.getEventTime()) {
			maxEventTime = e.getEventTime();
		}

		if (maxProcTime < e.getIngestionTime()) {
			maxProcTime = e.getIngestionTime();
		}
	}

	public TransactionWrapper median() {
		if (lowerHalfCount == upperHalfCount) {
			return lowerHalfCount == 0? null : average.apply(maxOfLowerHalf, lowestOfUpperHalf);
		}
		else {
			return maxOfLowerHalf;
		}
	}

	public int count() {
		return upperHalfCount + lowerHalfCount;
	}

	public synchronized void reset() {
		maxOfLowerHalf = null;
		lowestOfUpperHalf = null;

		lowerHalfCount = 0;
		upperHalfCount = 0;

		maxProcTime = 0L;
		maxEventTime = 0L;
	}
}
