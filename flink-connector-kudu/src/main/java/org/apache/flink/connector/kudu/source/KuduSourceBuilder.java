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
import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Builder to construct {@link KuduSource}.
 *
 * @param <OUT> type of the output records read from Kudu
 */
public class KuduSourceBuilder<OUT> {
    private KuduReaderConfig readerConfig;
    private KuduTableInfo tableInfo;
    private RowResultConverter<OUT> rowResultConverter;
    private Duration period;

    public KuduSourceBuilder<OUT> setTableInfo(KuduTableInfo tableInfo) {
        this.tableInfo = tableInfo;
        return this;
    }

    public KuduSourceBuilder<OUT> setReaderConfig(KuduReaderConfig readerConfig) {
        this.readerConfig = readerConfig;
        return this;
    }

    public KuduSourceBuilder<OUT> setRowResultConverter(
            RowResultConverter<OUT> rowResultConverter) {
        this.rowResultConverter = rowResultConverter;
        return this;
    }

    public KuduSourceBuilder<OUT> setPeriod(Duration period) {
        this.period = period;
        return this;
    }

    public KuduSource<OUT> build() {
        checkArgument(tableInfo != null, "Table info must be provided.");
        checkArgument(readerConfig != null, "Reader config must be provided.");
        checkArgument(rowResultConverter != null, "RowResultConverter must be provided.");
        checkArgument(period != null, "Period must be provided.");

        return new KuduSource<>(readerConfig, tableInfo, rowResultConverter, period);
    }
}
