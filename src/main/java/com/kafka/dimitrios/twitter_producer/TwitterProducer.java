package com.kafka.dimitrios.twitter_producer;

import com.google.common.collect.Lists;
import com.kafka.dimitrios.common.config.KafkaConfigDemo;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class TwitterProducer {

    public void twitterClient() {
        String consumerKey = "vYZmhqabWLvFqA7fo0QPEnwXr";
        String consumerSecret = "p4ZIksnlmGXOzaB6wUrlmxwabqkCFaZNLebI2gDH95tpQPCAJu";
        String token = "807218539-mE0bbOcCdNsSYjCVtTICTgoVd3Q3rnp7DvRoNVhH";
        String secret = "AhDaeFTU7qqlRzROCJKoOds2qGZRGSXcgE5lHqNyEXUZM";

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("kafka","lockdown");
        hosebirdEndpoint.trackTerms(terms);

        //// Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        //List<String> terms = Lists.newArrayList("twitter", "api");
        //hosebirdEndpoint.followings(followings);
        //hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret,
                token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
// Attempts to establish a connection.
        hosebirdClient.connect();
        KafkaProducer<String, String> kafkaProducer = kafkaProducer();
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (Objects.nonNull(msg)) {
                log.info("Twitter produce message :{}", msg);
               kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                   if(Objects.nonNull(e)){
                       log.error("Error",e);
                   }
                   log.info("Topic: {} " ,recordMetadata.topic());
               });
                //profit();
            }
        }
    }


    public KafkaProducer<String,String> kafkaProducer(){
        KafkaConfigDemo kafkaConfigDemo=new KafkaConfigDemo();
        kafkaConfigDemo.setProducerProperties();
        Properties properties = kafkaConfigDemo.getProperties();
        KafkaProducer<String, String> kafkaProducer =
                new KafkaProducer<String, String>(properties);
        return kafkaProducer;

    }

}
