/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.type.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.weakref.jmx.internal.guava.base.CaseFormat;

public class ElasticsearchClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, ElasticsearchTable>>> schemas;
    private final ElasticsearchConfig config;

    @Inject
    public ElasticsearchClient(ElasticsearchConfig config)
            throws IOException
    {
        requireNonNull(config, "config is null");

        this.config = config;
        schemas = Suppliers.memoize(schemasSupplier(config.getMetadata()));
    }

    public URI getMetadataURI() {
        return config.getMetadata();
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, ElasticsearchTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public ElasticsearchTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, ElasticsearchTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, ElasticsearchTable>>> schemasSupplier(final URI metadataUri)
    {
        return () -> {
            try {
                return lookupSchemas(metadataUri);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    private static Map<String, Map<String, ElasticsearchTable>> lookupSchemas(URI metadataUri)
            throws IOException
    {
        URL result = metadataUri.toURL();

        HttpHost host = new HttpHost(result.getHost(), result.getPort());
        RestClient restClient = RestClient.builder(host).build();

        Response response = restClient.performRequest("GET", "/_mappings",
                Collections.singletonMap("pretty", "true"));

        ObjectMapper m = new ObjectMapper();
        JsonNode rootNode = m.readTree(EntityUtils.toString(response.getEntity()));
        Iterator it = rootNode.fields();

        ImmutableList.Builder<ElasticsearchIndex> esIndexes = new ImmutableList.Builder<ElasticsearchIndex>();

        String re = "^([0-9]+)gs.*$";
        Pattern pattern = Pattern.compile(re);

        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = (Map.Entry<String, JsonNode>) it.next();
            String indexName = e.getKey();
            if(!indexName.matches(re)) {
                continue;
            }
            JsonNode indexNode = e.getValue();

            esIndexes.add(readIndex(indexName, indexNode));
        }

        ImmutableList<ElasticsearchIndex> indexList = esIndexes.build();

        ImmutableMap.Builder<String, List<ElasticsearchTable>> catalog = new ImmutableMap.Builder<String, List<ElasticsearchTable>>();
        List<ElasticsearchTable> schemaTables = new ArrayList<ElasticsearchTable>();

        for(ElasticsearchIndex esIndex : indexList) {
            for(ElasticsearchTable table : esIndex.getTables()) {
                if(!schemaTables.contains(table)) {
                    schemaTables.add(table);
                }
            }

            Matcher matcher = pattern.matcher(esIndex.getName());
            matcher.find();
//            String schemaName = String.format("assets_%s", matcher.group(1));
            String schemaName = String.format("assets_%s", esIndex.getName());
            catalog.put(schemaName, ImmutableList.copyOf(schemaTables));
        }

        return ImmutableMap.copyOf(transformValues(catalog.build(), resolveAndIndexTables(metadataUri)));
    }

    private static ElasticsearchIndex readIndex(String indexName, JsonNode indexNode) {
        ElasticsearchIndex index = new ElasticsearchIndex(indexName);

        ImmutableList.Builder<ElasticsearchTable> esTables = new ImmutableList.Builder<ElasticsearchTable>();

        JsonNode mappingsNode = indexNode.get("mappings");
        Iterator mit = mappingsNode.fields();
        while (mit.hasNext()) {
            Map.Entry<String, JsonNode> mappingEntity = (Map.Entry<String, JsonNode>) mit.next();

            String tableName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, mappingEntity.getKey());
            JsonNode tableNode = mappingEntity.getValue();

            esTables.add(readTable(tableName, tableNode));
        }

        index.setTables(esTables.build());

        return index;
    }

    private static ElasticsearchTable readTable(String tableName, JsonNode tableNode) {
        JsonNode propertiesNode = tableNode.get("properties");

        ImmutableList.Builder<ElasticsearchColumn> columns = new ImmutableList.Builder<ElasticsearchColumn>();

//        columns.add(new ElasticsearchColumn("customer_id", convertPropType("long")));

        Iterator it = propertiesNode.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = (Map.Entry<String, JsonNode>) it.next();
            String propName = e.getKey();
            JsonNode propNode = e.getValue();
            String propType = propNode.get("type").asText("");

            columns.add(new ElasticsearchColumn(propName, convertPropType(propType)));
        }

        return new ElasticsearchTable(tableName, columns.build());
    }

    private static Type convertPropType(String propType) {
        switch(propType.toLowerCase()) {
            case "integer":
                return IntegerType.INTEGER;
            case "long":
                return BigintType.BIGINT;
            case "double":
                return DoubleType.DOUBLE;
            case "string":
                return VarcharType.VARCHAR;
            case "boolean":
                return BooleanType.BOOLEAN;
            case "date":
                return VarcharType.VARCHAR;
        }

        throw new IllegalArgumentException("Unknown propType: " + propType);
    }

    private static Function<List<ElasticsearchTable>, Map<String, ElasticsearchTable>> resolveAndIndexTables(final URI metadataUri)
    {
        return tables -> {
            Iterable<ElasticsearchTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, ElasticsearchTable::getName));
        };
    }

    private static Function<ElasticsearchTable, ElasticsearchTable> tableUriResolver(final URI baseUri)
    {
        return table -> {
            return new ElasticsearchTable(table.getName(), table.getColumns());
        };
    }
}
