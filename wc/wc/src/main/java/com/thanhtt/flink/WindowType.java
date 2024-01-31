package com.thanhtt.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * This class represents a streaming word count application.
 * It reads text from a socket and counts the occurrences of words starting with
 * 'N'.
 */
public class WindowType {

    public static void main(String[] args) {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // read from socket
        DataStream<String> data = env.socketTextStream("localhost", 9090);

        // ts, amount
        DataStream<Tuple2<Long, Long>> mapped = data.map(new Splitter()); // tuple
                                                                          // [June,Category5,Bat,12,1]

        // [ts,amount]
        DataStream<Tuple2<Long, Long>> reduced = mapped
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, Long>>() {
                    public long extractAscendingTimestamp(Tuple2<Long, Long> element) {
                        return element.f0;
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new Reduce1());

        reduced.addSink(StreamingFileSink
                .forRowFormat(new Path("/opt/flink/code/code/wc/wc/input/WindowType.txt"),
                        new SimpleStringEncoder<Tuple2<Long, Long>>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        try {
            env.execute("Streaming WordCount");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static class Splitter implements MapFunction<String, Tuple2<Long, Long>> {
        public Tuple2<Long, Long> map(String value) {
            String[] words = value.split(",");
            // ignore timestamp, we don't need it for any calculations
            return new Tuple2<Long, Long>(Long.parseLong(words[0]),
                    Long.parseLong(words[1]));
        }
    }

    public static class Reduce1 implements ReduceFunction<Tuple2<Long, Long>> {
        public Tuple2<Long, Long> reduce(
                Tuple2<Long, Long> current,
                Tuple2<Long, Long> pre_result) {
            return new Tuple2<Long, Long>(current.f0,
                    current.f1 + pre_result.f1);
        }
    }
}
