/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kudu.source;

import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.KuduTestBase;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.util.HybridTimeUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** A test base class for {@link KuduSource} related tests. */
public class KuduSourceTestBase extends KuduTestBase {
    private static KuduReaderConfig readerConfig;
    private static KuduTableInfo tableInfo;
    private static final String tableName = "test_table";
    public static int lastInsertedKey = 0;

    @BeforeEach
    public void init() {
        readerConfig = KuduReaderConfig.Builder.setMasters(getMasterAddress()).build();
        tableInfo = KuduTableInfo.forTable(getTableName());

        try {
            createExampleTable();
            insertRows(10);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        try {
            deleteTable();
            lastInsertedKey = 0;
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    public static KuduReaderConfig getReaderConfig() {
        return readerConfig;
    }

    public static KuduTableInfo getTableInfo() {
        return tableInfo;
    }

    public static String getTableName() {
        return tableName;
    }

    public static int getTestRowsCount() {
        return lastInsertedKey;
    }

    public static void createExampleTable() throws KuduException {
        KuduClient client = getClient();
        // Set up a simple schema.
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true).build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("key");
        int numBuckets = 2;
        cto.addHashPartitions(hashKeys, numBuckets);

        client.createTable(tableName, schema, cto);
    }

    public static void deleteTable() throws KuduException {
        if (getClient().tableExists(tableName)) {
            getClient().deleteTable(tableName);
        }
    }

    public static void insertRows(int numRows) throws KuduException {
        KuduTable table = getClient().openTable(tableName);
        KuduSession session = getClient().newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        for (int i = 0; i < numRows; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            int currentKey = lastInsertedKey++; // Increment key to ensure uniqueness
            row.addInt("key", currentKey);
            // Make even-keyed row have a null 'value'.
            if (i % 2 == 0) {
                row.setNull("value");
            } else {
                row.addString("value", "value " + i);
            }
            session.apply(insert);
        }

        session.close();
        if (session.countPendingErrors() != 0) {
            throw new RuntimeException("Error inserting rows to Kudu");
        }
    }

    // Helper method to get the current Hybrid Time
    public long getCurrentHybridTime() {
        long fromMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        return HybridTimeUtil.physicalAndLogicalToHTTimestamp(fromMicros, 0) + 1;
    }
}
