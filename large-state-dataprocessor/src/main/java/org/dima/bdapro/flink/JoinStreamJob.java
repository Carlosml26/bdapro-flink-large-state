package org.dima.bdapro.flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.flink.datalayer.json.TransactionDeserializationSchema;

import org.dima.bdapro.utils.PropertiesHandler;

import java.util.Properties;

import static org.dima.bdapro.utils.Constants.SUBSCRIBER_TRANSACTION_PROFILE;
import static org.dima.bdapro.utils.Constants.TOPUP_PROFILE;

public class JoinStreamJob {

    private static StreamExecutionEnvironment STREAM_EXECUTION_ENVIRONMENT;

    public static void main(String[] args) throws Exception {
        Properties props = PropertiesHandler.getInstance(args != null && args.length > 1 ? args[0] : "large-state-dataprocessor/src/main/conf/flink-processor.properties").getModuleProperties();

        DataStream<Transaction> trasactionStream = initConsumer(props);
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

    private static void calculateRewardedSubscribers(DataStream<Transaction> transactionStream, Properties props) {

        DataStream<Tuple2<Transaction, Long>> ct = transactionStream
                .map(new MapFunction<Transaction, Tuple2<Transaction, Long>>() {
                    @Override
                    public Tuple2<Transaction, Long> map(Transaction transaction) throws Exception {
                        return new Tuple2<>(transaction, System.currentTimeMillis());
                    }
                });

        DataStream<Tuple2<Transaction, Long>> rts = ct
                .filter(x -> x.f0.getProfileId().equals(TOPUP_PROFILE));

        DataStream<Tuple2<Transaction, Long>> sts = ct
                .filter(x -> x.f0.getProfileId().equals(SUBSCRIBER_TRANSACTION_PROFILE));

        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        DataStream<Tuple2<Long, Long>> joinedDataStream = rts.join(sts)
                .where(new KeySelector<Tuple2<Transaction, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<Transaction, Long> transactionLongTuple2) throws Exception {
                        return transactionLongTuple2.f0.getReceiverId();
                    }
                })
                .equalTo(new KeySelector<Tuple2<Transaction, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<Transaction, Long> transactionLongTuple2) throws Exception {
                        return transactionLongTuple2.f0.getSenderId();
                    }
                })
                .window(TumblingEventTimeWindows.of(
                        Time.seconds(
                                Integer.parseInt(
                                        props.getProperty("flink.query.join_per_subcriberid.time_interval_join_size_seconds")
                                )
                        )
                    )
                ).apply(new JoinWindowFunction()).keyBy(0)
                .reduce(new ReduceTransactionFunction())
                .process(new ProcessFunction<Tuple6<String, Double, String, Double, Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public void processElement(Tuple6<String, Double, String, Double, Long, Long> t, Context context, Collector<Tuple2<Long, Long>> collector) throws Exception {
                        if (t.f3 >= 0.4*t.f1){
                            context.output(outputTag, t.f2);
                        }

                        long eventLatency = System.currentTimeMillis()-t.f4;
                        long procLatency = System.currentTimeMillis()-t.f5;

                        collector.collect(new Tuple2<Long, Long>(eventLatency, procLatency));
                    }
                });

        joinedDataStream.writeAsCsv("latency_query_join.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        //joinedDataStream.keyBy(0).map(new JoinLatencyMap()).writeAsCsv("latency_query_join.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    }

}

class JoinWindowFunction implements JoinFunction<Tuple2<Transaction, Long>, Tuple2<Transaction, Long>, Tuple6<String, Double, String, Double, Long, Long>> {
    @Override
    public Tuple6<String, Double, String, Double, Long, Long> join(Tuple2<Transaction, Long> t1, Tuple2<Transaction, Long> t2) throws Exception {
        Long maxEvent, maxProc;
        if (t1.f0.getTransactionTime()>t2.f0.getTransactionTime()){
            maxEvent = t1.f0.getTransactionTime();
            maxProc = t1.f1;
        }
        else{
            maxEvent = t2.f0.getTransactionTime();
            maxProc = t2.f1;
        }

        return new Tuple6<>(t1.f0.getTransactionId(), t1.f0.getTransactionAmount(), t2.f0.getSenderId(), t2.f0.getTransactionAmount(), maxEvent, maxProc);
    }
}


class ReduceTransactionFunction implements ReduceFunction<Tuple6<String, Double, String, Double, Long, Long>>{
    @Override
    public Tuple6<String, Double, String, Double, Long, Long> reduce(Tuple6<String, Double, String, Double, Long, Long> t0, Tuple6<String, Double, String, Double, Long, Long> t1)  {
        Long maxEvent, maxProc;

        if (t0.f4>t1.f4){
            maxEvent = t0.f4;
            maxProc = t0.f5;
        }
        else{
            maxEvent = t1.f4;
            maxProc = t1.f5;
        }
        return new Tuple6<>(t0.f0,t0.f1,t1.f2, t0.f3+t1.f3, maxEvent, maxProc);
    }
}
