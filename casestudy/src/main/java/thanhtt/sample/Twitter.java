package thanhtt.sample;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class Twitter {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checking input parameters
        // final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        // env.getConfig().setGlobalJobParameters(params);
        Properties props = new Properties();

        // flink_app_thanhtt
        props.setProperty(TwitterSource.CONSUMER_KEY, "brf3sUOHJuSp9A6IIBZVHv8b2");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "0NLux8pKcRi143dvFX4e1XpK06rzmDXSpU0pwtlZyVLKRp5HFs");
        props.setProperty(TwitterSource.TOKEN, "1422599106237665288-rwZeUlz4yQjIME4F26i1ECT4nnfSo8");
        props.setProperty(TwitterSource.TOKEN_SECRET, "1422599106237665288-rwZeUlz4yQjIME4F26i1ECT4nnfSo8");
        DataStream<String> streamSource = env.addSource(new TwitterSource(props));
        DataStream<JsonNode> parsedData = streamSource.map(new TweetParser());

        parsedData.print();

        env.execute("Twitter Analysis");
    }

    public static class TweetParser implements MapFunction<String, JsonNode> {

        public JsonNode map(String value) throws Exception {
            ObjectMapper jsonParser = new ObjectMapper();

            JsonNode node = jsonParser.readValue(value, JsonNode.class);
            return node;
        }
    }
}
