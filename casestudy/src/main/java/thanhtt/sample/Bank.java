package thanhtt.sample;

import java.io.File;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
// import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Bank {
    // data statefull save in MapState
    public static final MapStateDescriptor<String, AlarmedCustomer> alarmedCustStateDescriptor = new MapStateDescriptor<String, AlarmedCustomer>(
            "alarmed_customers", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(AlarmedCustomer.class));

    public static final MapStateDescriptor<String, LostCard> lostCardStateDescriptor = new MapStateDescriptor<String, LostCard>(
            "lost_cards", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(LostCard.class));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<AlarmedCustomer> alarmedCustomers = env
                .readTextFile("/opt/flink/code/code/casestudy/input/bank_data/alarmed_cust.txt")
                .map(new MapFunction<String, AlarmedCustomer>() {
                    public AlarmedCustomer map(String value) {
                        return new AlarmedCustomer(value);
                    }
                });

        // broadcast alarmed customer data
        BroadcastStream<AlarmedCustomer> alarmedCustBroadcast = alarmedCustomers.broadcast(alarmedCustStateDescriptor);

        DataStream<LostCard> lostCards = env
                .readTextFile("/opt/flink/code/code/casestudy/input/bank_data/lost_cards.txt")
                .map(new MapFunction<String, LostCard>() {
                    public LostCard map(String value) {
                        return new LostCard(value);
                    }
                });

        // broadcast lost card data
        BroadcastStream<LostCard> lostCardBroadcast = lostCards.broadcast(lostCardStateDescriptor);

        // transaction data keyed by customer_id
        DataStream<Tuple2<String, String>> data = env.socketTextStream("localhost", 9090)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    public Tuple2<String, String> map(String value) {
                        String[] words = value.split(",");

                        return new Tuple2<String, String>(words[3], value); // {(id_347hfx) (HFXR347924,2018-06-14
                                                                            // 23:32:23,Chandigarh,id_347hfx,hf98678167,123302773033,774
                    }
                });

        // (1) Check against alarmed customers
        DataStream<Tuple2<String, String>> alarmedCustTransactions = data
                .keyBy(t -> t.f0)
                .connect(alarmedCustBroadcast)
                .process(new AlarmedCustCheck());

        // (2) check against lost cards
        DataStream<Tuple2<String, String>> lostTransactions = data
                .keyBy(t -> t.f0)
                .connect(lostCardBroadcast)
                .process(new LostCardCheck());

        DataStream<Tuple2<String, String>> excessiveTransactions = data
                // (3) More than 10 transactions check
                .map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>() {
                    public Tuple3<String, String, Integer> map(Tuple2<String, String> value) {
                        return new Tuple3<String, String, Integer>(value.f0, value.f1, 1);
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(2)
                .flatMap(new FilterAndMapMoreThan10());

        // 4: Check against city change
        DataStream<Tuple2<String, String>> freqCityChangeTransactions = data
                // .keyBy(t -> t.f0)
                .keyBy(new KeySelector<Tuple2<String, String>, String>() {
                    public String getKey(Tuple2<String, String> value) {
                        return value.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new Citychange());

        DataStream<Tuple2<String, String>> AllFlaggedTxn = alarmedCustTransactions.union(lostTransactions,
                excessiveTransactions, freqCityChangeTransactions);

        AllFlaggedTxn.sinkTo(FileSink
                .forRowFormat(new Path("/opt/flink/code/code/casestudy/output/flagged_transaction"),
                        new SimpleStringEncoder<Tuple2<String, String>>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        env.execute("Streaming Bank");
    }

    public static class AlarmedCustCheck extends
            KeyedBroadcastProcessFunction<String, Tuple2<String, String>, AlarmedCustomer, Tuple2<String, String>> {
        /**
         * KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>
         * * @param <KS> The key type of the input keyed stream.
         * 
         * @param <IN1> The input type of the keyed (non-broadcast) side. (la
         *              transaction record)
         * @param <IN2> The input type of the broadcast side. (broadcasted alarmed
         *              customer data)
         * @param <OUT> The output type of the operator. (out tai : processElement)
         */
        public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx,
                Collector<Tuple2<String, String>> out) throws Exception {
            /**
             * This method is called for each element in the input data stream. It compares
             * the customer ID of the current transaction with the IDs of all alarmed
             * customers. If a match is found, an alarm is triggered.
             *
             * @param value the current transaction
             * @param ctx   the current context
             * @param out   the collector for emitting results
             * @throws Exception if an error occurs
             */

            for (Map.Entry<String, AlarmedCustomer> custEntry : ctx.getBroadcastState(alarmedCustStateDescriptor)
                    .immutableEntries()) {
                final String alarmedCustId = custEntry.getKey();
                final AlarmedCustomer cust = custEntry.getValue();

                // get customer_id of current transaction
                final String tId = value.f1.split(",")[3];
                if (tId.equals(alarmedCustId)) {
                    out.collect(new Tuple2<String, String>("____ALARM___",
                            "Transaction: " + value + " is by an ALARMED customer"));
                }
            }

        }

        public void processBroadcastElement(AlarmedCustomer cust, Context ctx, Collector<Tuple2<String, String>> out)
                throws Exception {
            /**
             * This method is called for each element in the broadcast stream. It stores the
             * alarmed customer data in the broadcast state.
             *
             * @param cust the alarmed customer data
             * @param ctx  the current context
             * @param out  the collector for emitting results
             * @throws Exception if an error occurs
             */
            ctx.getBroadcastState(alarmedCustStateDescriptor).put(cust.id, cust);
        }
    }

    public static class LostCardCheck
            extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, LostCard, Tuple2<String, String>> {
        public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx,
                Collector<Tuple2<String, String>> out) throws Exception {
            for (Map.Entry<String, LostCard> cardEntry : ctx.getBroadcastState(lostCardStateDescriptor)
                    .immutableEntries()) {
                final String lostCardId = cardEntry.getKey();
                final LostCard card = cardEntry.getValue();

                // get card_id of current transaction
                final String cId = value.f1.split(",")[5];
                if (cId.equals(lostCardId)) {
                    out.collect(
                            new Tuple2<String, String>("__ALARM__", "Transaction: " + value + " issued via LOST card"));
                }
            }
        }

        public void processBroadcastElement(LostCard card, Context ctx, Collector<Tuple2<String, String>> out)
                throws Exception {
            ctx.getBroadcastState(lostCardStateDescriptor).put(card.id, card);
        }
    }

    public static class FilterAndMapMoreThan10
            implements FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>> {
        public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) {
            if (value.f2 > 10) {
                out.collect(new Tuple2<String, String>("__ALARM__", value + " marked for >10 TXNs"));
            }
        }
    }

    public static class Citychange
            extends ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow> {
        public void process(String key, Context context, Iterable<Tuple2<String, String>> input,
                Collector<Tuple2<String, String>> out) {
            String lastCity = "";
            int changeCount = 0;
            for (Tuple2<String, String> element : input) {
                String city = element.f1.split(",")[2].toLowerCase();

                if (lastCity.isEmpty()) {
                    lastCity = city;
                } else {
                    if (!city.equals(lastCity)) {
                        lastCity = city;
                        changeCount += 1;
                    }
                }

                if (changeCount >= 2) {
                    out.collect(new Tuple2<String, String>("__ALARM__", element + "marked for FREQUENT city changes"));
                }
            }
        }
    }
}
