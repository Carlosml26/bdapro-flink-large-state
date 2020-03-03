package org.dima.bdapro.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
		env.setParallelism(Integer.parseInt(props.getProperty("flink.parallelism.default")));

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

		DataStream<Transaction> transactionStream = env.addSource(consumer).setParallelism(
				Integer.parseInt(props.getProperty("flink.parallelism.source")));

		DataStream<Tuple2<Transaction, Long>> ct = transactionStream
				.map(new MapFunction<Transaction, Tuple2<Transaction, Long>>() {
					@Override
					public Tuple2<Transaction, Long> map(Transaction transaction) throws Exception {
						return new Tuple2<>(transaction, System.currentTimeMillis());
					}
				})
				.filter(x -> x.f0.getProfileId().equals(RESELLER_TRANSACTION_PROFILE));

		SingleOutputStreamOperator<Tuple3<Long, Long, Long>> aggPerResellerId = ct.keyBy((KeySelector<Tuple2<Transaction, Long>, String>) x -> (args[0].equals("id"))?x.f0.getSenderId():x.f0.getSenderType())
				.timeWindow(Time.milliseconds(Integer.parseInt(props.getProperty("flink.query.agg.time_window_size_ms"))))
				.apply(new MedianWindowFunction())
				.map(new SendProcLatencyMap());

		aggPerResellerId.writeAsCsv(outputDir+"latency_query_sender_" +args[0]+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		//aggPerResellerId.map(x -> new Tuple3<String, Integer, Double>(x.f0, x.f1, x.f2)).writeAsCsv(outputDir+"result_query_sender_"+args[0]+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		// execute program
		env.execute("Aggregation Streaming Job");
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

class SendProcLatencyMap extends RichMapFunction<Tuple5<String, Integer, Double, Long, Long>, Tuple3<Long, Long, Long>>  {

	private transient double processingTimeValueGauge = 0;
	private transient double eventTimeValueGauge = 0;
	private transient Counter numberEventCount;
	private transient double processingTimeSum = 0;
	private transient double eventTimeSum = 0;


	@Override
	public void open(org.apache.flink.configuration.Configuration config) throws Exception {
		this.numberEventCount = getRuntimeContext()
				.getMetricGroup()
				.counter("eventCounter");


		getRuntimeContext()
				.getMetricGroup()
				.gauge("ProcessingLatencyGauge", new Gauge<Double>() {
					@Override
					public Double getValue() {
						return processingTimeValueGauge;
					}
				});

		getRuntimeContext()
				.getMetricGroup()
				.gauge("EventLatencyGauge", new Gauge<Double>() {
					@Override
					public Double getValue() {
						return eventTimeValueGauge;
					}
				});
	}


	@Override
	public Tuple3<Long, Long, Long> map(Tuple5<String, Integer, Double, Long, Long> t) throws Exception {
		long timestamp = System.currentTimeMillis();
		long eventLatency = timestamp-t.f3;
		long procLatency = timestamp-t.f4;

		numberEventCount.inc();
		eventTimeSum += eventLatency;
		processingTimeSum += procLatency;

		eventTimeValueGauge = eventTimeSum/numberEventCount.getCount();
		processingTimeValueGauge = processingTimeSum/numberEventCount.getCount();

		return new Tuple3<>(eventLatency, procLatency, t.f3);
	}

}
