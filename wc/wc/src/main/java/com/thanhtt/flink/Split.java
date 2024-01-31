package com.thanhtt.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Split {
    public static void main(String[] args) {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> data = env.readTextFile(params.get("input"));

        // String type side output for Even values
        final OutputTag<String> evenOutTag = new OutputTag<String>("even-string-output") {
        };
        // Integer type side output for Odd values
        final OutputTag<Integer> oddOutTag = new OutputTag<Integer>("odd-int-output") {
        };

        SingleOutputStreamOperator<Integer> mainStream = data.process(new ProcessFunction<String, Integer>() {
            @Override
            public void processElement(
                    String value,
                    Context ctx,
                    Collector<Integer> out) throws Exception {

                int intVal = Integer.parseInt(value);

                // get all data in regular output as well
                out.collect(intVal);

                if (intVal % 2 == 0) {
                    // emit data to side output for even output
                    ctx.output(evenOutTag, String.valueOf(intVal));
                } else {
                    // emit data to side output for even output
                    ctx.output(oddOutTag, intVal);
                }
            }
        });

        DataStream<String> evenSideOutputStream = mainStream.getSideOutput(evenOutTag);
        DataStream<Integer> oddSideOutputStream = mainStream.getSideOutput(oddOutTag);

        evenSideOutputStream.writeAsText("/opt/flink/code/code/wc/wc/input/even.txt");
        oddSideOutputStream.writeAsText("/opt/flink/code/code/wc/wc/input/odd.txt");

        try {
            env.execute("Streaming Split");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
