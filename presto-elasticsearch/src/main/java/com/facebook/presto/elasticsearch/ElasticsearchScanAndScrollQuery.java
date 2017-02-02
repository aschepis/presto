package com.facebook.presto.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * Created by aschepis on 2/2/17.
 */
public class ElasticsearchScanAndScrollQuery {
    private final RestClient restClient;
    private final String indexName;
    private final String assetType;
    private Iterator<JsonNode> recordIterator;

    private String scrollId;

    public ElasticsearchScanAndScrollQuery(RestClient restClient, String indexName, String assetType) {
        requireNonNull(restClient, "restClient is null");
        requireNonNull(indexName, "indexName is null");
        requireNonNull(assetType, "assetType is null");

        this.restClient = restClient;
        this.indexName = indexName;
        this.assetType = assetType;
    }

    public boolean fetchNextPage() throws IOException {
        String path;
        if (scrollId == null) {
            // http://localhost:9200/1/AwsAccount/_search?q=_type:AwsAccount&scroll=1m&size=100
            path = String.format("/%s/%s/_search?q=_type:%s&size=1000&scroll=1m", indexName, assetType, assetType);
        } else {
            path = String.format("/_search/scroll?scroll=1m&scroll_id=%s", scrollId);
        }

        Response esResponse = restClient.performRequest("GET", path, Collections.singletonMap("pretty", "true"));

        ObjectMapper m = new ObjectMapper();
        JsonNode rootNode = m.readTree(EntityUtils.toString(esResponse.getEntity()));
        JsonNode hitsNode = rootNode.get("hits");
        JsonNode hitListNode = hitsNode.get("hits");

        scrollId = rootNode.get("_scroll_id").asText();
        recordIterator = hitListNode.iterator();

        return hitListNode.isArray() && ((ArrayNode)hitListNode).size() > 0;
    }

    public Iterator<JsonNode> getRecordIterator() throws IOException {
        if(recordIterator == null) {
            fetchNextPage();
        }

        return recordIterator;
    }
}
