package org.dima.bdapro.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.flink.datalayer.json.TransactionDeserializationSchema;

import org.dima.bdapro.utils.LiveMedianCalculator;
import org.dima.bdapro.utils.PropertiesHandler;

import java.util.Properties;

import static org.dima.bdapro.utils.Constants.RESELLER_TRANSACTION_PROFILE;

public class AggregationStreamingJob {

	public static void main(String[] args) throws Exception {

		Properties props = PropertiesHandler.getInstance(args != null && args.length > 1 ? args[1] : "large-state-dataprocessor/src/main/conf/flink-processor.properties").getModuleProperties();
		String outputDir = "";
		if (args.length == 3){
			outputDir = args[2];
		}
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(Long.parseLong(props.getProperty("flink.checkpointing.delay")), CheckpointingMode.EXACTLY_ONCE);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(Integer.parseInt(props.getProperty("flink.parallelism")));

		FlinkKafkaConsumer<Transaction> consumer = new FlinkKafkaConsumer<Transaction>(
				props.getProperty("topic"),
				new TransactionDeserializationSchema(),
				props
		);

		consumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Transaction>(
				Time.milliseconds(Integer.parseInt(props.getProperty("flink.kafka.consumer.watermark-delay_milliseconds")))) {
			@Override
			public long extractTimestamp(Transaction element) {
				return element.getTransactionTime();
			}
		});

		if ("from_earliest".equals(props.getProperty("flink.kafka.consume"))) {
			consumer.setStartFromEarliest();
		}
		else {
			consumer.setStartFromLatest();
		}

		DataStream<Transaction> transactionStream = env.addSource(consumer);
		DataStream<Tuple2<Transaction, Long>> ct = transactionStream
				.map(new MapFunction<Transaction, Tuple2<Transaction, Long>>() {
					@Override
					public Tuple2<Transaction, Long> map(Transaction transaction) throws Exception {
						return new Tuple2<>(transaction, System.currentTimeMillis());
					}
				})// put timestamp for latency
				.filter(x -> x.f0.getProfileId().equals(RESELLER_TRANSACTION_PROFILE));

		DataStream<Tuple5<String,  Integer, Double, Long, Long>> aggPerResellerId = ct.keyBy((KeySelector<Tuple2<Transaction, Long>, String>) x -> (args[0].equals("id"))?x.f0.getSenderId():x.f0.getSenderType())
				.timeWindow(Time.milliseconds(Integer.parseInt(props.getProperty("flink.query.agg_per_ressellerId.time_window_size_ms"))))
				.apply(new MedianWindowFunction());

		aggPerResellerId.map(new MapFunction<Tuple5<String, Integer, Double, Long, Long>, Tuple3<Long, Long, Long>>() {
			@Override
			public Tuple3<Long, Long, Long> map(Tuple5<String, Integer, Double, Long, Long> t) throws Exception {
				long timestamp = System.currentTimeMillis();
				long eventLatency = timestamp-t.f3;
				long procLatency = timestamp-t.f4;
				return new Tuple3<>(eventLatency, procLatency, t.f3);
			}
		}).writeAsCsv(outputDir+"latency_query_sender_" +args[0]+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		aggPerResellerId.map(new MapFunction<Tuple5<String, Integer, Double, Long, Long>, Tuple3<String, Integer, Double>>() {
			@Override
			public Tuple3<String, Integer, Double> map(Tuple5<String, Integer, Double, Long, Long> x) throws Exception {
				return new Tuple3<String, Integer, Double>(x.f0, x.f1, x.f2);
			}
		}).writeAsCsv(outputDir+"result_query_sender_"+args[0]+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

}

class MedianWindowFunction implements WindowFunction<Tuple2<Transaction, Long>, Tuple5<String, Integer, Double, Long, Long>, String, TimeWindow> {

	@Override
	public void apply(String s, TimeWindow window, Iterable<Tuple2<Transaction, Long>> elements, Collector<Tuple5<String, Integer, Double, Long, Long>> out) throws Exception {

		LiveMedianCalculator<Transaction> medianCalculator = new LiveMedianCalculator<>((x, y) -> x.getTransactionAmount().compareTo(y.getTransactionAmount()),
				(x, y) -> {
					x.setTransactionAmount((x.getTransactionAmount() + y.getTransactionAmount()) / 2);
					return x;
				});

		Long maxEventTime = 0L;
		Long maxProcTime = 0L;

		for (Tuple2<Transaction, Long> t : elements) {
			medianCalculator.add(t.f0);

			if (maxEventTime < t.f0.getTransactionTime()) {
				maxEventTime = t.f0.getTransactionTime();
			}
			if (maxProcTime < t.f1){
				maxProcTime = t.f1;
			}
		}

		out.collect(new Tuple5<>(s, medianCalculator.count(), medianCalculator.median().getTransactionAmount(), maxEventTime, maxProcTime));

	}
}