package org.dima.bdapro.utils;

import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.TransactionWrapper;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.BiFunction;

/**
 * An online way of calculating median. Implementation is inspired from:
 * https://stackoverflow.com/questions/11955728/how-to-calculate-the-median-of-an-array#answer-50570427
 *
 * This implementation is tuned for out {@link TransactionWrapper}
 */
public class TransactionMedianCalculatorWithQueue {

	private BiFunction<TransactionWrapper, TransactionWrapper, Integer> comparator;
	private BiFunction<TransactionWrapper, TransactionWrapper, TransactionWrapper> average;


	private PriorityQueue<TransactionWrapper> low = new PriorityQueue<>(new TComparator().reversed());
	private PriorityQueue<TransactionWrapper> high = new PriorityQueue<>(new TComparator());

	private long maxProcTime;
	private long maxEventTime;


	public TransactionMedianCalculatorWithQueue() {

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
		Queue<TransactionWrapper> target = low.size() <= high.size() ? low : high;
		target.add(e);


		if (maxEventTime < e.getEventTime()) {
			maxEventTime = e.getEventTime();
		}

		if (maxProcTime < e.getIngestionTime()) {
			maxProcTime = e.getIngestionTime();
		}
	}

	private void balance() {
		while(!low.isEmpty() && !high.isEmpty() && comparator.apply(low.peek() , high.peek()) > 0) {
			TransactionWrapper lowHead= low.poll();
			TransactionWrapper highHead = high.poll();
			low.add(highHead);
			high.add(lowHead);
		}
	}

	public TransactionWrapper median() {
		if (low.size() == high.size()) {
			return low.size() == 0? null : average.apply(low.peek(), high.peek());
		}
		else {
			return low.peek();
		}
	}

	public int count() {
		return low.size() + high.size();
	}

	public synchronized void reset() {
		low.clear();
		high.clear();

		maxProcTime = 0L;
		maxEventTime = 0L;
	}

	class TComparator implements Comparator<TransactionWrapper> {

		@Override
		public int compare(TransactionWrapper o1, TransactionWrapper o2) {
			return comparator.apply(o1, o2);
		}
	}
}

