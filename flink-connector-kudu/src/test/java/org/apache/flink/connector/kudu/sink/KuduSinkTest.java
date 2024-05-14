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

package org.apache.flink.connector.kudu.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.KuduTestBase;
import org.apache.flink.connector.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connector.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.types.Row;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KuduSink}. */
public class KuduSinkTest extends KuduTestBase {

    private static final String[] columns = new String[] {"id", "uuid"};

    @Test
    void testInvalidWriterConfig() {
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);

        KuduSinkBuilder<Row> builder =
                KuduSink.<Row>builder()
                        .setTableInfo(tableInfo)
                        .setOperationMapper(initOperationMapper(columns));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Writer config");
    }

    @Test
    void testInvalidTableInfo() {
        String masterAddresses = getMasterAddress();
        KuduWriterConfig writerConfig =
                KuduWriterConfig.Builder.setMasters(masterAddresses).build();

        KuduSinkBuilder<Row> builder =
                KuduSink.<Row>builder()
                        .setWriterConfig(writerConfig)
                        .setOperationMapper(initOperationMapper(columns));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Table info");
    }

    @Test
    void testInvalidOperationMapper() {
        String masterAddresses = getMasterAddress();
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        KuduWriterConfig writerConfig =
                KuduWriterConfig.Builder.setMasters(masterAddresses).build();

        KuduSinkBuilder<Row> builder =
                KuduSink.<Row>builder().setWriterConfig(writerConfig).setTableInfo(tableInfo);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Operation mapper");
    }

    @Test
    void testNotTableExist() {
        String masterAddresses = getMasterAddress();
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        KuduWriterConfig writerConfig =
                KuduWriterConfig.Builder.setMasters(masterAddresses).build();

        KuduSink<Row> sink =
                KuduSink.<Row>builder()
                        .setWriterConfig(writerConfig)
                        .setTableInfo(tableInfo)
                        .setOperationMapper(initOperationMapper(columns))
                        .build();

        assertThatThrownBy(() -> sink.createWriter(null)).isInstanceOf(RuntimeException.class);
    }

    @Test
    void testOutputWithStrongConsistency() throws Exception {
        String masterAddresses = getMasterAddress();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig =
                KuduWriterConfig.Builder.setMasters(masterAddresses).setStrongConsistency().build();

        KuduSink<Row> sink =
                KuduSink.<Row>builder()
                        .setWriterConfig(writerConfig)
                        .setTableInfo(tableInfo)
                        .setOperationMapper(initOperationMapper(KuduTestBase.columns))
                        .build();

        SinkWriter<Row> writer = sink.createWriter(null);

        for (Row kuduRow : booksDataRow()) {
            writer.write(kuduRow, null);
        }
        writer.close();

        List<Row> rows = readRows(tableInfo);

        assertThat(rows).hasSize(5);
        kuduRowsTest(rows);
    }

    @Test
    void testOutputWithEventualConsistency() throws Exception {
        String masterAddresses = getMasterAddress();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig =
                KuduWriterConfig.Builder.setMasters(masterAddresses)
                        .setEventualConsistency()
                        .build();

        KuduSink<Row> sink =
                KuduSink.<Row>builder()
                        .setWriterConfig(writerConfig)
                        .setTableInfo(tableInfo)
                        .setOperationMapper(initOperationMapper(KuduTestBase.columns))
                        .build();

        SinkWriter<Row> writer = sink.createWriter(null);

        for (Row kuduRow : booksDataRow()) {
            writer.write(kuduRow, null);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        writer.close();

        List<Row> rows = readRows(tableInfo);

        assertThat(rows).hasSize(5);
        kuduRowsTest(rows);
    }

    @Test
    void testSpeed() throws Exception {
        String masterAddresses = getMasterAddress();

        KuduTableInfo tableInfo =
                KuduTableInfo.forTable("test_speed")
                        .createTableIfNotExists(
                                () ->
                                        Lists.newArrayList(
                                                new ColumnSchema.ColumnSchemaBuilder(
                                                                "id", Type.INT32)
                                                        .key(true)
                                                        .build(),
                                                new ColumnSchema.ColumnSchemaBuilder(
                                                                "uuid", Type.STRING)
                                                        .build()),
                                () ->
                                        new CreateTableOptions()
                                                .setNumReplicas(3)
                                                .addHashPartitions(Lists.newArrayList("id"), 6));

        KuduWriterConfig writerConfig =
                KuduWriterConfig.Builder.setMasters(masterAddresses)
                        .setEventualConsistency()
                        .build();

        KuduSink<Row> sink =
                KuduSink.<Row>builder()
                        .setWriterConfig(writerConfig)
                        .setTableInfo(tableInfo)
                        .setOperationMapper(initOperationMapper(columns))
                        .build();

        SinkWriter<Row> writer = sink.createWriter(null);

        int totalRecords = 100000;
        for (int i = 0; i < totalRecords; i++) {
            Row kuduRow = new Row(2);
            kuduRow.setField(0, i);
            kuduRow.setField(1, UUID.randomUUID().toString());
            writer.write(kuduRow, null);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        writer.close();

        List<Row> rows = readRows(tableInfo);
        assertThat(rows).hasSize(totalRecords);
    }

    private RowOperationMapper initOperationMapper(String[] cols) {
        return new RowOperationMapper(cols, AbstractSingleOperationMapper.KuduOperation.INSERT);
    }
}
