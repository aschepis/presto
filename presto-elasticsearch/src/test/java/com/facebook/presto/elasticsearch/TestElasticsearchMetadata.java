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
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestElasticsearchMetadata
{
    private static final String CONNECTOR_ID = "TEST";
    private static final ElasticsearchTableHandle NUMBERS_TABLE_HANDLE = new ElasticsearchTableHandle(CONNECTOR_ID, "elasticsearch", "numbers");
    private ElasticsearchMetadata metadata;
    private URI metadataUri;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestElasticsearchClient.class, "/elasticsearch-data/elasticsearch-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        metadataUri = metadataUrl.toURI();
        ElasticsearchClient client = new ElasticsearchClient(new ElasticsearchConfig().setMetadata(metadataUri));
        metadata = new ElasticsearchMetadata(new ElasticsearchConnectorId(CONNECTOR_ID), client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableSet.of("elasticsearch", "tpch"));
    }

    @Test
    public void testGetTableHandle()
    {
        assertEquals(metadata.getTableHandle(SESSION, new SchemaTableName("elasticsearch", "numbers")), NUMBERS_TABLE_HANDLE);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("elasticsearch", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, NUMBERS_TABLE_HANDLE), ImmutableMap.of(
                "text", new ElasticsearchColumnHandle(CONNECTOR_ID, "text", createUnboundedVarcharType(), 0),
                "value", new ElasticsearchColumnHandle(CONNECTOR_ID, "value", BIGINT, 1)));

        // unknown table
        try {
            metadata.getColumnHandles(SESSION, new ElasticsearchTableHandle(CONNECTOR_ID, "unknown", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
        try {
            metadata.getColumnHandles(SESSION, new ElasticsearchTableHandle(CONNECTOR_ID, "elasticsearch", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, NUMBERS_TABLE_HANDLE);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("elasticsearch", "numbers"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("text", createUnboundedVarcharType()),
                new ColumnMetadata("value", BIGINT)));

        // unknown tables should produce null
        assertNull(metadata.getTableMetadata(SESSION, new ElasticsearchTableHandle(CONNECTOR_ID, "unknown", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new ElasticsearchTableHandle(CONNECTOR_ID, "elasticsearch", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new ElasticsearchTableHandle(CONNECTOR_ID, "unknown", "numbers")));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, null)), ImmutableSet.of(
                new SchemaTableName("elasticsearch", "numbers"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, "elasticsearch")), ImmutableSet.of(
                new SchemaTableName("elasticsearch", "numbers")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, "tpch")), ImmutableSet.of(
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, "unknown")), ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(metadata.getColumnMetadata(SESSION, NUMBERS_TABLE_HANDLE, new ElasticsearchColumnHandle(CONNECTOR_ID, "text", createUnboundedVarcharType(), 0)),
                new ColumnMetadata("text", createUnboundedVarcharType()));

        // elasticsearch connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // ElasticsearchTableHandle and ElasticsearchColumnHandle passed in.  This is on because
        // it is not possible for the Presto Metadata system to create the handles
        // directly.
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testCreateTable()
    {
        metadata.createTable(SESSION, new ConnectorTableMetadata(
                new SchemaTableName("elasticsearch", "foo"),
                ImmutableList.of(new ColumnMetadata("text", createUnboundedVarcharType()))));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testDropTableTable()
    {
        metadata.dropTable(SESSION, NUMBERS_TABLE_HANDLE);
    }
}
