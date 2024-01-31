package com.thanhtt.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount {
  public static void main(String[] args)
      throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    ParameterTool params = ParameterTool.fromArgs(args);

    env.getConfig().setGlobalJobParameters(params);

    DataSet<String> text = env.readTextFile(params.get("input"));

    DataSet<String> filtered = text.filter(new FilterFunction<String>() { // dataset of [N1 N2 N3]
      public boolean filter(String value) {
        return value.startsWith("N");
      }
    });

    DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer()); // [(N, 1), (N2, 1), (N3, 1)]

    DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);


    if (params.has("output")) {
      counts.writeAsCsv(params.get("output"), "\n", " ");

      env.execute("WordCount Example");
    }
  
  }

  public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
    public Tuple2<String, Integer> map(String value) {
      return new Tuple2(value, Integer.valueOf(1));
    }
  }
}
