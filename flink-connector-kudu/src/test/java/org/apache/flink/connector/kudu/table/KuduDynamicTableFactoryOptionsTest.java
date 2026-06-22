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

package org.apache.flink.connector.kudu.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for Kudu dynamic table sink options. */
class KuduDynamicTableFactoryOptionsTest {

    private static final String MASTERS = "localhost";
    private static final String TABLE_NAME = "TestTable";
    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("first", DataTypes.STRING()),
                    Column.physical("second", DataTypes.STRING()));

    @Test
    void testMaxMutationBufferOpsOptionConfiguresSink() {
        Map<String, String> properties = baseProperties();
        properties.put("sink.max-mutation-buffer-ops", "123");

        assertEquals(expectedSink(123), createSink(properties));
    }

    @Test
    void testDeprecatedMaxBufferSizeOptionStillConfiguresSink() {
        Map<String, String> properties = baseProperties();
        properties.put("sink.max-buffer-size", "456");

        assertEquals(expectedSink(456), createSink(properties));
    }

    private static Map<String, String> baseProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "kudu");
        properties.put("masters", MASTERS);
        properties.put("table-name", TABLE_NAME);
        return properties;
    }

    private static DynamicTableSink expectedSink(int maxMutationBufferOps) {
        KuduWriterConfig.Builder builder =
                KuduWriterConfig.Builder.setMasters(MASTERS)
                        .setMaxMutationBufferOps(maxMutationBufferOps);
        KuduTableInfo tableInfo = KuduTableInfo.forTable(TABLE_NAME);
        return new KuduDynamicTableSink(builder, tableInfo, SCHEMA);
    }

    private static DynamicTableSink createSink(Map<String, String> properties) {
        return FactoryUtil.createDynamicTableSink(
                null,
                ObjectIdentifier.of("kudu", "default", TABLE_NAME),
                new ResolvedCatalogTable(CatalogTable.fromProperties(properties), SCHEMA),
                properties,
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }
}
