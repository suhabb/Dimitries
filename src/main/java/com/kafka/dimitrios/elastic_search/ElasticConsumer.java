package com.kafka.dimitrios.elastic_search;

import com.kafka.dimitrios.common.config.KafkaConfigDemo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

@Slf4j
public class ElasticConsumer {

    private KafkaConfigDemo kafkaConfigDemo;

    private ElasticSearch elasticSearch;

    public ElasticConsumer() {
        this.kafkaConfigDemo = new KafkaConfigDemo();
        this.elasticSearch = new ElasticSearch();
    }

    public void createElasticConsumer() throws IOException {
        this.kafkaConfigDemo.setConsumerProperties("my-elastic-consumer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(this.kafkaConfigDemo.getProperties());

        kafkaConsumer.subscribe(Arrays.asList("twitter_tweets"));

        RestHighLevelClient elasticClient = elasticSearch.createElasticClient();

        while (true) {

            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord consumerRecord : consumerRecords) {
                log.info("Consumer Records: \n" +
                                " Key :{} , Value: {},Partition:{},OffSet: {}", consumerRecord.key(), consumerRecord.value(),
                        consumerRecord.partition(), consumerRecord.offset());

                IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                        .source(consumerRecord.value(), XContentType.JSON);
                IndexResponse indexResponse = elasticClient.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                log.info("id:{}", id);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
         //   elasticClient.close();
        }
    }
}
