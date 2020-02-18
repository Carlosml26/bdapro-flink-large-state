package org.dima.bdapro.utils;

//import com.sun.istack.internal.NotNull;
import org.dima.bdapro.datalayer.bean.Transaction;

import java.util.stream.Stream;

public class LiveMedianCalcRunner {
	public static void main(String[] args) {

//		LiveMedianCalculator<Double> doubleMedianCalc = new LiveMedianCalculator<Double>();
//
//		Stream.of(9.0d, 1.0d, 1.5d, 5.0d, 2.3d)
//				.forEach(doubleMedianCalc::add);
//
//		System.out.println(doubleMedianCalc.median()); // 2.3
//
//		doubleMedianCalc.add(2.2d);
//
//		System.out.println(doubleMedianCalc.median()); // 2.25
//
//
//		LiveMedianCalculator<MyData> customMedianCalc = new LiveMedianCalculator<MyData>() {
//			@Override
//			public MyData average(MyData e1, MyData e2) {
//				return MyData.average(e1, e2);
//			}
//		};
//
//		Stream.of(9.0d, 1.0d, 1.5d, 5.0d, 2.3d)
//				.map(e -> new MyData(e, e.toString()))
//				.forEach(customMedianCalc::add);
//
//		System.out.println(customMedianCalc.median()); // 2.3
//
//		customMedianCalc.add(new MyData(2.2d, "2.2"));
//
//		System.out.println(customMedianCalc.median()); // 2.25


	}
}

class MyData implements Comparable<MyData> {

	private Double x;
	private String a;

	MyData(Double x, String a) {
		this.x = x;
		this.a = a;
	}

//	@NotNull
	@Override
	public int compareTo(MyData o) {
		return x != null ? x.compareTo(o.x) : -1;
	}

	@Override
	public String toString() {
		return "MyData{" +
				"x=" + x +
				", a='" + a + '\'' +
				'}';
	}

	public static MyData average(MyData e1, MyData e2) {
		return new MyData((e1.x + e2.x) / 2, e1.a + "+" + e2.a);
	}
}