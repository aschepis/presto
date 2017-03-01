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

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordSet
        implements RecordSet
{
    private final List<ElasticsearchColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final ElasticsearchScanAndScrollQuery query;

    public ElasticsearchRecordSet(ElasticsearchSplit split, List<ElasticsearchColumnHandle> columnHandles) throws IOException
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ElasticsearchColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        HostAddress metadataHost = split.getAddresses().get(0);
        HttpHost host = new HttpHost(metadataHost.getHostText(), metadataHost.getPortOrDefault(9200));
        RestClient restClient = RestClient.builder(host).build();

        Pattern indexPattern = Pattern.compile("^assets_([0-9]+)gs.*$");
        Matcher matcher = indexPattern.matcher(split.getSchemaName());
        matcher.find();
        String esAliasName = matcher.group(1);
        String assetType = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, split.getTableName());

        query = new ElasticsearchScanAndScrollQuery(restClient, esAliasName, assetType);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new ElasticsearchRecordCursor(columnHandles, query);
    }
}
