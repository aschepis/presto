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

import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.presto.elasticsearch.MetadataUtil.TABLE_CODEC;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestElasticsearchTable
{
    private final ElasticsearchTable elasticsearchTable = new ElasticsearchTable("tableName",
            ImmutableList.of(new ElasticsearchColumn("a", createUnboundedVarcharType()), new ElasticsearchColumn("b", BIGINT)));

    @Test
    public void testColumnMetadata()
    {
        assertEquals(elasticsearchTable.getColumnsMetadata(), ImmutableList.of(
                new ColumnMetadata("a", createUnboundedVarcharType()),
                new ColumnMetadata("b", BIGINT)));
    }

    @Test
    public void testRoundTrip()
            throws Exception
    {
        String json = TABLE_CODEC.toJson(elasticsearchTable);
        ElasticsearchTable elasticsearchTableCopy = TABLE_CODEC.fromJson(json);

        assertEquals(elasticsearchTableCopy.getName(), elasticsearchTable.getName());
        assertEquals(elasticsearchTableCopy.getColumns(), elasticsearchTable.getColumns());
    }
}
