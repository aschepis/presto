package com.facebook.presto.elasticsearch;

import com.google.common.collect.ImmutableList;

/**
 * Created by aschepis on 1/31/17.
 */
public class ElasticsearchIndex {
    private final String name;
    private ImmutableList<ElasticsearchTable> tables;

    public ElasticsearchIndex(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public ImmutableList<ElasticsearchTable> getTables() {
        return tables;
    }

    public void setTables(ImmutableList<ElasticsearchTable> tables) {
        this.tables = tables;
    }
}
