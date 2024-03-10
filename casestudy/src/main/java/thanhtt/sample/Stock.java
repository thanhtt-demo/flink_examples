package thanhtt.sample;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Stock {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

        WatermarkStrategy<Tuple5<String, String, String, Double, Integer>> ws = WatermarkStrategy
                .<Tuple5<String, String, String, Double, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        Timestamp ts = new Timestamp(sdf.parse(event.f0 + " " + event.f1).getTime());
                        return ts.getTime();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    // should never reach here ...
                    return 0L;
                });

        DataStream<Tuple5<String, String, String, Double, Integer>> data = env
                .readTextFile("/opt/flink/code/code/casestudy/input/stock_data/FUTURES_TRADES.txt")
                .map(new MapFunction<String, Tuple5<String, String, String, Double, Integer>>() {
                    public Tuple5<String, String, String, Double, Integer> map(String value) {
                        String[] words = value.split(",");
                        // date, time, Name, trade, volume
                        return new Tuple5<String, String, String, Double, Integer>(words[0], words[1], "XYZ",
                                Double.parseDouble(words[2]), Integer.parseInt(words[3]));
                    }
                })
                .assignTimestampsAndWatermarks(ws);

        // Compute per window statistics
        DataStream<String> change = data
                .keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>() {
                    public String getKey(Tuple5<String, String, String, Double, Integer> value) {
                        return value.f2;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new TrackChange());

        change.sinkTo(FileSink
                .forRowFormat(new Path("/opt/flink/code/code/casestudy/output/Ist report.txt"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        // Alert when price change from one window to another is more than threshold
        DataStream<String> largeDelta = data
                .keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>() {
                    public String getKey(Tuple5<String, String, String, Double, Integer> value) {
                        return value.f2;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new TrackLargeDelta(5));

        largeDelta.sinkTo(FileSink.forRowFormat(new Path("/opt/flink/code/code/casestudy/output/Alert.txt"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        env.execute("Stock Analysis");

    }

    public static class TrackChange
            extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {
        private transient ValueState<Double> prevWindowMaxTrade;
        private transient ValueState<Integer> prevWindowMaxVol;

        public void process(String key, Context context,
                Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out)
                throws Exception {
            String windowStart = "";
            String windowEnd = "";
            Double windowMaxTrade = 0.0; // 0
            Double windowMinTrade = 0.0; // 106
            Integer windowMaxVol = 0;
            Integer windowMinVol = 0; // 348746

            for (Tuple5<String, String, String, Double, Integer> element : input)
            // 06/10/2010, 08:00:00, 106.0, 348746
            // 06/10/2010, 08:00:00, 105.0, 331580
            {
                if (windowStart.isEmpty()) {
                    windowStart = element.f0 + ":" + element.f1; // 06/10/2010 : 08:00:00
                    windowMinTrade = element.f3;
                    windowMinVol = element.f4;
                }
                if (element.f3 > windowMaxTrade)
                    windowMaxTrade = element.f3;

                if (element.f3 < windowMinTrade)
                    windowMinTrade = element.f3;

                if (element.f4 > windowMaxVol)
                    windowMaxVol = element.f4;
                if (element.f4 < windowMinVol)
                    windowMinVol = element.f4;

                windowEnd = element.f0 + ":" + element.f1;
            }

            Double maxTradeChange = 0.0;
            Double maxVolChange = 0.0;

            if (prevWindowMaxTrade != null && prevWindowMaxTrade.value() != 0) {
                maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) / prevWindowMaxTrade.value()) * 100;
            }
            if (prevWindowMaxVol != null && prevWindowMaxVol.value() != 0)
                maxVolChange = ((windowMaxVol - prevWindowMaxVol.value()) * 1.0 / prevWindowMaxVol.value()) * 100;

            out.collect(windowStart + " - " + windowEnd + ", " + windowMaxTrade + ", " + windowMinTrade + ", "
                    + String.format("%.2f", maxTradeChange) +
                    ", " + windowMaxVol + ", " + windowMinVol + ", " + String.format("%.2f", maxVolChange));

            prevWindowMaxTrade.update(windowMaxTrade);
            prevWindowMaxVol.update(windowMaxVol);
        }

        public void open(Configuration config) {
            prevWindowMaxTrade = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Double>("prev_max_trade", BasicTypeInfo.DOUBLE_TYPE_INFO,Double.valueOf(0)));

            prevWindowMaxVol = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Integer>("prev_max_vol", BasicTypeInfo.INT_TYPE_INFO,Integer.valueOf(0)));
        }
    }

    public static class TrackLargeDelta
            extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {
        private final double threshold;
        private transient ValueState<Double> prevWindowMaxTrade;

        public TrackLargeDelta(double threshold) {
            this.threshold = threshold;
        }

        public void process(String key, Context context,
                Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out)
                throws Exception {
            Double prevMax = 0.0;
            if (prevWindowMaxTrade.value() != null) {
                prevMax = prevWindowMaxTrade.value();
            }
            Double currMax = 0.0;
            String currMaxTimeStamp = "";

            for (Tuple5<String, String, String, Double, Integer> element : input) {
                if (element.f3 > currMax) {
                    currMax = element.f3;
                    currMaxTimeStamp = element.f0 + ":" + element.f1;
                }
            }

            // check if change is more than specified threshold
            Double maxTradePriceChange = ((currMax - prevMax) / prevMax) * 100;

            if (prevMax != 0 && // don't calculate delta the first time
                    Math.abs((currMax - prevMax) / prevMax) * 100 > threshold) {
                out.collect("Large Change Detected of " + String.format("%.2f", maxTradePriceChange) + "%" + " ("
                        + prevMax + " - " + currMax + ") at  " + currMaxTimeStamp);
            }
            prevWindowMaxTrade.update(currMax);
        }

        public void open(Configuration config) {
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<Double>("prev_max",
                    BasicTypeInfo.DOUBLE_TYPE_INFO, Double.valueOf(0));
            prevWindowMaxTrade = getRuntimeContext().getState(descriptor);
        }
    }
}
