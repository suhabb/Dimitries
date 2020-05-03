package com.kafka.dimitrios.twitter_producer;

public class TwitterMain {

    public static void main(String[] args) {
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.twitterClient();
    }
}
