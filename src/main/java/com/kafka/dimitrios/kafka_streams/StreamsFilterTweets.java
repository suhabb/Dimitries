package com.kafka.dimitrios.kafka_streams;

import com.google.gson.JsonParser;
import com.kafka.dimitrios.common.config.KafkaConfigDemo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

@Slf4j
public class StreamsFilterTweets {

    public void createStreamApp() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();


        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");

        KStream<String, String> kStream = inputTopic.filter(
                (k, jsonTweet) -> extractUserFromTweet(jsonTweet) > 1000
        );
        //filter
        kStream.to("important_tweets");//topic_name

        //start streams application
        KafkaConfigDemo kafkaConfigDemo = new KafkaConfigDemo();
        kafkaConfigDemo.setStreamProperties();
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kafkaConfigDemo.getProperties());
        kafkaStreams.start();
    }


    public Integer extractUserFromTweet(String tweetJson) {
        log.info("-------------------Json:{}",tweetJson);
        try {
            JsonParser jsonParser = new JsonParser();
           // jsonParser.
            final int followersCount = jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
            log.info("Followers Count : {}", followersCount);
            return followersCount;
        } catch (Exception e) {
            return 0;
        }
    }
}
