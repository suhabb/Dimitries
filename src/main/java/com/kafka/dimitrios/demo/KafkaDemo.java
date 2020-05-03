package com.kafka.dimitrios.demo;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaDemo {
    public static void main(String[] args) {
        ProducerCreateDemo producerCreateDemo = new ProducerCreateDemo();
        producerCreateDemo.createProducer();
      // ConsumerDemo consumerDemo = new ConsumerDemo();
       // consumerDemo.createConsumer();
    }
}
