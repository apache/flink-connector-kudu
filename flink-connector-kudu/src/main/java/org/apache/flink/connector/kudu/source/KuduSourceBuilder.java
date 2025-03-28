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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder to construct {@link KuduSource}.
 *
 * @param <OUT> type of the output records read from Kudu
 */
public class KuduSourceBuilder<OUT> {
    private KuduReaderConfig readerConfig;
    private KuduTableInfo tableInfo;
    private Boundedness boundedness;
    private Duration discoveryPeriod;
    private RowResultConverter<OUT> rowResultConverter;

    KuduSourceBuilder() {
        boundedness = Boundedness.BOUNDED;
    }

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

    public KuduSourceBuilder<OUT> setBoundedness(Boundedness boundedness) {
        this.boundedness = boundedness;
        return this;
    }

    public KuduSourceBuilder<OUT> setDiscoveryPeriod(Duration discoveryPeriod) {
        this.discoveryPeriod = discoveryPeriod;
        return this;
    }

    public KuduSource<OUT> build() {
        checkNotNull(tableInfo, "Table info must be provided.");
        checkNotNull(readerConfig, "Reader config must be provided.");
        checkNotNull(rowResultConverter, "RowResultConverter must be provided.");
        if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED) {
            checkNotNull(
                    discoveryPeriod,
                    "Discovery period must be provided for CONTINUOUS_UNBOUNDED mode.");
        }

        return new KuduSource<>(
                readerConfig, tableInfo, boundedness, discoveryPeriod, rowResultConverter);
    }
}
