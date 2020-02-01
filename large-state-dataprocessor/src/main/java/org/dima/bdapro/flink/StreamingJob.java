package org.dima.bdapro.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
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

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		Properties props = PropertiesHandler.getInstance(args != null && args.length > 1 ? args[0] : "src/main/conf/flink-processor.properties").getModuleProperties();

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

		DataStream<Transaction> trasactionStream = env.addSource(consumer);
		DataStream<Transaction> ct = trasactionStream
				.filter(x -> x.getProfileId().equals(RESELLER_TRANSACTION_PROFILE));

//		ct.print();

		DataStream<Tuple3<String, Double, Integer>> aggPerResellerId = ct.keyBy((KeySelector<Transaction, String>) Transaction::getSenderId)
				.timeWindow(Time.milliseconds(Integer.parseInt(props.getProperty("flink.query.agg_per_ressellerId.time_window_size_ms"))))
				.apply(new MedianWindowFunction());

		aggPerResellerId.print("reseller_id");


		DataStream<Tuple3<String, Double, Integer>> aggPerResellerType = ct.keyBy((KeySelector<Transaction, String>) Transaction::getSenderType)
				.timeWindow(Time.milliseconds(Integer.parseInt(props.getProperty("flink.query.agg_per_ressellerType.time_window_size_ms"))))
				.apply(new MedianWindowFunction());

		aggPerResellerType.print("reseller_type");


		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

}

class MedianWindowFunction implements WindowFunction<Transaction, Tuple3<String, Double, Integer>, String, TimeWindow> {
	@Override
	public void apply(String s, TimeWindow window, Iterable<Transaction> elements, Collector<Tuple3<String, Double, Integer>> out) throws Exception {

		LiveMedianCalculator<Transaction> medianCalculator = new LiveMedianCalculator<>((x, y) -> x.getTransactionAmount().compareTo(y.getTransactionAmount()),
				(x, y) -> {
					x.setTransactionAmount((x.getTransactionAmount() + y.getTransactionAmount()) / 2);
					return x;
				});

		for (Transaction t : elements) {
			medianCalculator.add(t);
		}

		out.collect(new Tuple3<>(s, medianCalculator.median().getTransactionAmount(), medianCalculator.count()));

	}
}