package com.kafka.dimitrios.kafka_streams;

public class StreamsMain {

    public static void main(String[] args) {
        StreamsFilterTweets streamsFilterTweets = new StreamsFilterTweets();

        //streamsFilterTweets.createStreamApp();

        WordCountApp wordCountApp = new WordCountApp();
        wordCountApp.wordCount();
    }
}
