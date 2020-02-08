package org.dima.bdapro.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
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
import static org.dima.bdapro.utils.Constants.SUBSCRIBER_TRANSACTION_PROFILE;
import static org.dima.bdapro.utils.Constants.TOPUP_PROFILE;

public class StreamingJob {

	private static StreamExecutionEnvironment STREAM_EXECUTION_ENVIRONMENT;

	public static void main(String[] args) throws Exception {
		Properties props = PropertiesHandler.getInstance(args != null && args.length > 1 ? args[0] : "../large-state-dataprocessor/src/main/conf/flink-processor.properties").getModuleProperties();

		DataStream<Transaction> trasactionStream = initConsumer(props);
//		calculateResellerUsageStatistics(trasactionStream, props);
		calculateRewardedSubscribers(trasactionStream, props);


		// execute program
		STREAM_EXECUTION_ENVIRONMENT.execute("Flink Streaming Java API Skeleton");
	}


	private static DataStream<Transaction> initConsumer(Properties props) {

		// set up the streaming execution environment
		STREAM_EXECUTION_ENVIRONMENT = StreamExecutionEnvironment.getExecutionEnvironment();
		STREAM_EXECUTION_ENVIRONMENT.enableCheckpointing(Long.parseLong(props.getProperty("flink.checkpointing.delay")), CheckpointingMode.EXACTLY_ONCE);
		STREAM_EXECUTION_ENVIRONMENT.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		STREAM_EXECUTION_ENVIRONMENT.setParallelism(Integer.parseInt(props.getProperty("flink.parallelism")));


		FlinkKafkaConsumer<Transaction> consumer = new FlinkKafkaConsumer<>(
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

		return STREAM_EXECUTION_ENVIRONMENT.addSource(consumer);
	}

	private static void calculateResellerUsageStatistics(DataStream<Transaction> trasactionStream, Properties props) {

		DataStream<Transaction> ct = trasactionStream
				.filter(x -> x.getProfileId().equals(RESELLER_TRANSACTION_PROFILE) || x.getProfileId().equals(TOPUP_PROFILE));

//		ct.print();

		DataStream<Tuple3<String, Double, Integer>> aggPerResellerId = ct.keyBy((KeySelector<Transaction, String>) Transaction::getSenderId)
				.timeWindow(Time.milliseconds(Integer.parseInt(props.getProperty("flink.query.agg_per_ressellerId.time_window_size_ms"))))
				.apply(new MedianWindowFunction());

		aggPerResellerId.print("reseller_id");


		DataStream<Tuple3<String, Double, Integer>> aggPerResellerType = ct.keyBy((KeySelector<Transaction, String>) Transaction::getSenderType)
				.timeWindow(Time.milliseconds(Integer.parseInt(props.getProperty("flink.query.agg_per_ressellerType.time_window_size_ms"))))
				.apply(new MedianWindowFunction());

		aggPerResellerType.print("reseller_type");

	}

	private static void calculateRewardedSubscribers(DataStream<Transaction> trasactionStream, Properties props) {

		DataStream<Transaction> rts = trasactionStream
				.filter(x -> x.getProfileId().equals(TOPUP_PROFILE));

		DataStream<Transaction> sts = trasactionStream
				.filter(x -> x.getProfileId().equals(SUBSCRIBER_TRANSACTION_PROFILE));


		DataStream<String> names = rts.keyBy(Transaction::getReceiverId)
				.intervalJoin(sts.keyBy(Transaction::getSenderId))
				.between(Time.milliseconds(0), Time.seconds(Integer.parseInt(props.getProperty("flink.query.join_per_subcriberid.time_interval_join_size_seconds"))))
				.process(new JoinWindowFunction ())
				.keyBy(0)
				.reduce(new ReduceTransactionFunction())
				.filter( t -> t.f3 >= 0.4*t.f1)
				.map(t -> t.f2);

		names.print("Subcriber_id");
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


class JoinWindowFunction extends ProcessJoinFunction<Transaction, Transaction, Tuple4<String,Double, String,Double>> {
	@Override
	public void processElement(Transaction rts, Transaction sts, Context context, Collector<Tuple4<String,Double,String, Double>> collector)  {
		if (sts.getTransactionAmount() >= 0.4*rts.getTransactionAmount())
			collector.collect(new Tuple4<>(rts.getTransactionId(),rts.getTransactionAmount(),sts.getSenderId(), sts.getTransactionAmount()));
	}
}


class ReduceTransactionFunction implements ReduceFunction<Tuple4<String, Double, String, Double>>{
	@Override
	public Tuple4<String, Double, String, Double> reduce(Tuple4<String, Double, String, Double> t0, Tuple4<String, Double, String, Double> t1)  {
		return new Tuple4<>(t0.f0,t0.f1,t1.f2, t0.f3+t1.f3);
	}
}
