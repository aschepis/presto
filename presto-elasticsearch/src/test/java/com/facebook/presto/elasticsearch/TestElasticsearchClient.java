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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestElasticsearchClient
{
    @Test
    public void testMetadata()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestElasticsearchClient.class, "/elasticsearch-data/elasticsearch-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadata = metadataUrl.toURI();
        ElasticsearchClient client = new ElasticsearchClient(new ElasticsearchConfig().setMetadata(metadata));
        assertEquals(client.getSchemaNames(), ImmutableSet.of("elasticsearch", "tpch"));
        assertEquals(client.getTableNames("elasticsearch"), ImmutableSet.of("numbers"));
        assertEquals(client.getTableNames("tpch"), ImmutableSet.of("orders", "lineitem"));

        ElasticsearchTable table = client.getTable("elasticsearch", "numbers");
        assertNotNull(table, "table is null");
        assertEquals(table.getName(), "numbers");
        assertEquals(table.getColumns(), ImmutableList.of(new ElasticsearchColumn("text", createUnboundedVarcharType()), new ElasticsearchColumn("value", BIGINT)));
    }
}
