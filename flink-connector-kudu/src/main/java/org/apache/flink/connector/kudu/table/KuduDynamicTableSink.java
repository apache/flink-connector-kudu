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

import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connector.kudu.connector.writer.RowDataUpsertOperationMapper;
import org.apache.flink.connector.kudu.sink.KuduSink;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** A {@link KuduDynamicTableSink} for Kudu. */
public class KuduDynamicTableSink implements DynamicTableSink {
    private final KuduWriterConfig.Builder writerConfigBuilder;
    private final KuduTableInfo tableInfo;
    private final ResolvedSchema flinkSchema;

    public KuduDynamicTableSink(
            KuduWriterConfig.Builder writerConfigBuilder,
            KuduTableInfo tableInfo,
            ResolvedSchema flinkSchema) {
        this.writerConfigBuilder = writerConfigBuilder;
        this.tableInfo = tableInfo;
        this.flinkSchema = flinkSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        this.validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || tableInfo.getSchema().getPrimaryKeyColumnCount() != 0,
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        KuduSink<RowData> upsertKuduSink =
                KuduSink.<RowData>builder()
                        .setWriterConfig(writerConfigBuilder.build())
                        .setTableInfo(tableInfo)
                        .setOperationMapper(new RowDataUpsertOperationMapper(flinkSchema))
                        .build();
        return SinkV2Provider.of(upsertKuduSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new KuduDynamicTableSink(writerConfigBuilder, tableInfo, flinkSchema);
    }

    @Override
    public String asSummaryString() {
        return "kudu";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KuduDynamicTableSink that = (KuduDynamicTableSink) o;
        return Objects.equals(writerConfigBuilder, that.writerConfigBuilder)
                && Objects.equals(flinkSchema, that.flinkSchema)
                && Objects.equals(tableInfo, that.tableInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writerConfigBuilder, flinkSchema, tableInfo);
    }
}
