package com.thanhtt.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.thanhtt.flink.AvgProfit.Reduce1;

public class Assignment_Cab {
    public static void main(String[] args) {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> data = env.readTextFile(params.get("input"));

        // cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,1,passenger count
        DataStream<Tuple8<String, String, String, String, String, String, String, String>> filter_mapped = data
                .map(new Splitter()).filter(new OngoingFilter());

        DataStream<Tuple8<String, String, String, String, String, String, String, Integer>> mapped = filter_mapped.map(
                new MapFunction<Tuple8<String, String, String, String, String, String, String, String>, Tuple8<String, String, String, String, String, String, String, Integer>>() {
                    public Tuple8<String, String, String, String, String, String, String, Integer> map(
                            Tuple8<String, String, String, String, String, String, String, String> input) {
                        return new Tuple8<>(input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, input.f6,
                                Integer.parseInt(input.f7));
                    }// cast to int
                });

        DataStream<Tuple2<String, Integer>> sum_by_destination = mapped.map(new DestinationGroupMap()).keyBy(value -> value.f0) // group by key
                .sum(1); // sum by key
        System.out.println("sum_by_destination: ");
        sum_by_destination.print();

        // Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.
        DataStream<Tuple4<String, Integer, Integer, Double>> avgPassengerPickupLocation = mapped.map(new PickupLocationGroupMap())
                .keyBy(value -> value.f0).reduce(new CumsumPassengerPerPickupLocation())
                .map(new AvgCumsumPassengerPerPickupLocation());
        System.out.println("avgPassengerPickupLocation");
        avgPassengerPickupLocation.print();

        sum_by_destination.writeAsText("/opt/flink/code/code/wc/wc/input/sum_by_destination.txt");
        avgPassengerPickupLocation.writeAsText("/opt/flink/code/code/wc/wc/input/avgPassengerPickupLocation.txt");

        try {
            env.execute("Streaming Assignment Cab");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static class Splitter
            implements MapFunction<String, Tuple8<String, String, String, String, String, String, String, String>> {
        public Tuple8<String, String, String, String, String, String, String, String> map(String value) {
            String[] words = value.split(",");
            // ignore timestamp, we don't need it for any calculations
            return new Tuple8<String, String, String, String, String, String, String, String>(words[0],
                    words[1],
                    words[2],
                    words[3],
                    words[4],
                    words[5],
                    words[6],
                    words[7]);
        }
    }

    // MapFunction that adds two integer values
public static class DestinationGroupMap implements MapFunction<Tuple8<String, String, String, String, String, String, String, Integer>, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(
            Tuple8<String, String, String, String, String, String, String, Integer> value) throws Exception {
        // TODO Auto-generated method stub

        return new Tuple2<>(value.f6, 1);
    }
    
  }

  public static class PickupLocationGroupMap implements MapFunction<Tuple8<String, String, String, String, String, String, String, Integer>, Tuple3<String, Integer, Integer>> {

    @Override
    public Tuple3<String, Integer, Integer> map(
            Tuple8<String, String, String, String, String, String, String, Integer> value) throws Exception {
        // pickup location,passenger count,1
        return new Tuple3<>(value.f5, 7,1);
    }
    
  }

  public static class CumsumPassengerPerPickupLocation implements ReduceFunction<Tuple3<String, Integer, Integer>> {
      public Tuple3<String, Integer, Integer> reduce(
              Tuple3<String, Integer, Integer> current,
              Tuple3<String, Integer, Integer> pre_result) {
          // pickup_location,total_passenger_count,same_localtion_count
          return new Tuple3<String, Integer, Integer>(current.f0,
                  current.f1 + pre_result.f1,
                  current.f2 + pre_result.f2);
      }
  }

  public static class AvgCumsumPassengerPerPickupLocation
          implements MapFunction<Tuple3<String, Integer, Integer>, Tuple4<String, Integer, Integer, Double>> {

      @Override
      public Tuple4<String, Integer, Integer, Double> map(
              Tuple3<String, Integer, Integer> value) throws Exception {
          // pickup_location,total_passenger_count,same_localtion_count
          return new Tuple4<>(value.f0, value.f1, value.f2, (double) value.f1 / value.f2);
      }
    
  }

    public static class ReduceTotalPassenger implements ReduceFunction<Tuple8<String, String, String, String, String, String, String, Integer>> {
        public Tuple8<String, String, String, String, String, String, String, Integer> reduce(
                Tuple8<String, String, String, String, String, String, String, Integer> current,
                Tuple8<String, String, String, String, String, String, String, Integer> pre_result) {
            return new Tuple8<String, String, String, String, String, String, String, Integer>(current.f0,
                    current.f1,
                    current.f2,
                    current.f3,
                    current.f4,
                    current.f5,
                    current.f6,
                    current.f7 + pre_result.f7);
        }
    }

        // FilterFunction that filters out all Integers smaller than zero.
        public static class OngoingFilter
                implements FilterFunction<Tuple8<String, String, String, String, String, String, String, String>> {
            @Override
            public boolean filter(Tuple8<String, String, String, String, String, String, String, String> value) {

                return value.f4.equals("yes");
            }
        }
}
