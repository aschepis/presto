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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class ElasticsearchRecordCursor
        implements RecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final List<ElasticsearchColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;
    private final ElasticsearchScanAndScrollQuery query;

    private long bytesRead;

    private JsonNode currentRecord;

    public ElasticsearchRecordCursor(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchScanAndScrollQuery query)
    {
        this.columnHandles = columnHandles;
        this.query = query;

        bytesRead = 0;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            ElasticsearchColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
    }

    @Override
    public long getTotalBytes()
    {
        return 0L;
    } // Unknown

    @Override
    public long getCompletedBytes()
    {
        return bytesRead;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (!query.getRecordIterator().hasNext()) {
                boolean hasNextPage = query.fetchNextPage();
                if (!hasNextPage) {
                    return false;
                }
            }

            currentRecord = query.getRecordIterator().next().get("_source");
            return true;
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
        return false;
    }

    private String getFieldValue(int field)
    {
        checkState(currentRecord != null, "Cursor has not been advanced yet");

        ElasticsearchColumnHandle columnHandle = columnHandles.get(field);
        JsonNode valueNode = currentRecord.get(columnHandle.getColumnName());
        return valueNode == null || valueNode.isNull() ? null : valueNode.asText();
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s(%s) to be type %s but is %s", field, columnHandles.get(field).getColumnName(), expected, actual);
    }

    @Override
    public void close()
    {
    }
}
