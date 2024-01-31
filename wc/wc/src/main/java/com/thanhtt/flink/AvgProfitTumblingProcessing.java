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

/**
 * This class represents a streaming word count application.
 * It reads text from a socket and counts the occurrences of words starting with
 * 'N'.
 */
public class AvgProfitTumblingProcessing {
    
    public static void main(String[] args) {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // read from socket
        DataStream < String > data = env.socketTextStream("localhost", 9090);

        // month, product, category, profit, count
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter()); // tuple
                                                                                                        // [June,Category5,Bat,12,1]

        // [June,Category4,Perfume,10,1]
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped.keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce(new Reduce1());

        reduced.addSink(StreamingFileSink
        .forRowFormat(new Path("/opt/flink/code/code/wc/wc/input/output_AvgProfitTumblingProcessing.txt"),
            new SimpleStringEncoder < Tuple5 < String, String, String, Integer, Integer >> ("UTF-8"))
        .withRollingPolicy(DefaultRollingPolicy.builder().build())
        .build());

        try {
            env.execute("Streaming WordCount");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
        public Tuple5<String, String, String, Integer, Integer> map(String value) // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(","); // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations
            return new Tuple5<String, String, String, Integer, Integer>(words[1],
                    words[2],
                    words[3],
                    Integer.parseInt(words[4]),
                    1);
        } // (June,Category5,Bat,12,1)
    }

    public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
        public Tuple5<String, String, String, Integer, Integer> reduce(
                Tuple5<String, String, String, Integer, Integer> current,
                Tuple5<String, String, String, Integer, Integer> pre_result) {
            return new Tuple5<String, String, String, Integer, Integer>(current.f0,
                    current.f1,
                    current.f2,
                    current.f3 + pre_result.f3,
                    current.f4 + pre_result.f4);
        }
    }
}
