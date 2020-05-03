package com.kafka.dimitrios.demo;

import com.kafka.dimitrios.common.config.KafkaConfigDemo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

@Slf4j
public class ProducerCreateDemo {

    private KafkaConfigDemo kafkaConfigDemo;

    public ProducerCreateDemo() {
        this.kafkaConfigDemo = new KafkaConfigDemo();
    }

    public void createProducer() {
        kafkaConfigDemo.setProducerProperties();
        Properties properties = kafkaConfigDemo.getProperties();
        KafkaProducer<String, String> kafkaProducer =
                new KafkaProducer<String, String>(properties);
        for(int index=0;index<10;index++) {
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_tweets","id_"+index,
                    "Message from program producer"+ index);
            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Received new meta data - " +
                                        "Topic : {} ," +
                                        "Partition : {}," +
                                        " Offset : {} ," +
                                        " Timestamp:"+
                                        recordMetadata .timestamp(),
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset());
                    } else {
                        log.error("Error :", e);
                    }
                }
            });
        }
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
