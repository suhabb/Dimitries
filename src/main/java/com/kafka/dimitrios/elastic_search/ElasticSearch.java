package com.kafka.dimitrios.elastic_search;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

@Slf4j
public class ElasticSearch {


    public void createIndex() throws IOException {
        RestHighLevelClient elasticClient = createElasticClient();
        String jsonString = "{\"foo\":\"bar\"}";
        IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                .source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = elasticClient.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        log.info("Elastic Id:{}", id);
        elasticClient.close();
    }


    public RestHighLevelClient createElasticClient() {
        //https://75ak43nbqv:xpseaf8p2x@szt-course-2370882712.eu-central-1.bonsaisearch.net:443
        //https://app.bonsai.io/clusters/szt-course-2370882712/console
        String hostname = "szt-course-2370882712.eu-central-1.bonsaisearch.net";
        String username = "75ak43nbqv";
        String password = "xpseaf8p2x";
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;

    }
}
