package com.kafka.dimitrios.common.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaConfigDemo {

    private Properties properties;

    public KafkaConfigDemo(){
         this.properties = new Properties();

    }

    public void setProducerProperties(){
        this.properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        this.properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create safe producer
        this.properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        this.properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        this.properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        this.properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        //high throughput at teh expense of latency and CPU usage
        this.properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        this.properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        this.properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));


    }

    public void setConsumerProperties(){
        this.properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        this.properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        this.properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-sixth-group");
        this.properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

    }

    public void setConsumerProperties(String groupId){
        this.properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        this.properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        this.properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        this.properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    }

    public void setStreamProperties(){
        this.properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        this.properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-stream-group");
        this.properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        this.properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());

    }
    public Properties getProperties(){
        return this.properties;
    }


}
