package org.dima.bdapro.utils;

import java.util.function.BiFunction;

/**
 * Provides an online low memory and fast implementation of median.
 * Solution inspired from: https://stackoverflow.com/questions/11955728/how-to-calculate-the-median-of-an-array/59905644#59905644
 * @param <T>
 */
public class LiveMedianCalculator<T> {

	private BiFunction<T, T, Integer> comparator;
	private BiFunction<T, T, T> average;

	private T maxOfLowerHalf;
	private T lowestOfUpperHalf;

	private int lowerHalfCount;
	private int upperHalfCount;

	public LiveMedianCalculator(BiFunction<T, T, Integer> comparator, BiFunction<T, T, T> average) {
		this.comparator = comparator;
		this.average = average;
	}

	/**
	 * Add a new element.
	 * @param e
	 */
	public void add(T e) {
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
			T x = maxOfLowerHalf;
			maxOfLowerHalf = lowestOfUpperHalf;
			lowestOfUpperHalf = x;
		}
	}

	public T median() {
		if (lowerHalfCount == upperHalfCount) {
			return average.apply(maxOfLowerHalf, lowestOfUpperHalf);
		}
		else {
			return maxOfLowerHalf;
		}
	}

	public int count() {
		return upperHalfCount + lowerHalfCount;
	}
}
