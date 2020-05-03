package com.kafka.dimitrios.demo;

import com.kafka.dimitrios.common.config.KafkaConfigDemo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;

@Slf4j
public class ConsumerDemo {

    private KafkaConfigDemo kafkaConfigDemo;

    public ConsumerDemo(){
        this.kafkaConfigDemo = new KafkaConfigDemo();
    }

    public void createConsumer(){
        this.kafkaConfigDemo.setConsumerProperties();
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(this.kafkaConfigDemo.getProperties());

        kafkaConsumer.subscribe(Arrays.asList("first_topic"));

        //poll for new data

        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord consumerRecord:consumerRecords){
                log.info("Consumer Records: \n" +
                        " Key :{} , Value: {},Partition:{},OffSet: {}",consumerRecord.key(),consumerRecord.value(),
                        consumerRecord.partition(),consumerRecord.offset());
            }
        }
    }
}
