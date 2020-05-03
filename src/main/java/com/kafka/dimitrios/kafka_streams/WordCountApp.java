package com.kafka.dimitrios.kafka_streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class WordCountApp {


    public void wordCount(){
        StreamConfig streamConfig = new StreamConfig();
        Properties configProperties = streamConfig.setProperties();
        //1.Stream from kafka
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> wordCountStream = streamsBuilder.stream("word-count-input");

        //2.map values to lowercase
        KTable<String, Long> wordCountTable = wordCountStream
                .mapValues((value) -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" "))) //split by space
                .selectKey((ignoredKey, word) -> word) // discard the old key and return new key - word
                .groupByKey()
                .count("Counts");

        wordCountTable.to(Serdes.String(),Serdes.Long(),"word-count-output");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder,configProperties);
        kafkaStreams.start();
        log.info("Topology :{}",kafkaStreams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
