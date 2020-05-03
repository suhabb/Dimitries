package com.kafka.dimitrios.elastic_search;

import java.io.IOException;

public class ElasticMain {

    public static void main(String[] args) throws IOException {
        ElasticSearch elasticSearch = new ElasticSearch();
        elasticSearch.createIndex();

        ElasticConsumer elasticConsumer = new ElasticConsumer();
        elasticConsumer.createElasticConsumer();
    }
}
