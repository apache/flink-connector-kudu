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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumerator;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumeratorState;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumeratorStateSerializer;
import org.apache.flink.connector.kudu.source.reader.KuduSourceReader;
import org.apache.flink.connector.kudu.source.reader.KuduSourceSplitReader;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.time.Duration;

/**
 * A continuous, unbounded Flink {@link Source} for reading data from Apache Kudu. It uses
 * differential scanning to achieve a CDC-like behavior, continuously capturing changes.
 *
 * <p>Key components:
 *
 * <ul>
 *   <li>{@link KuduReaderConfig} - Configures the Kudu connection, including master addresses.
 *   <li>{@link KuduTableInfo} - Specifies the target Kudu table, including its name and schema
 *       details.
 *   <li>{@link RowResultConverter} - Converts Kudu's {@code RowResult} into the desired output type
 *       {@code OUT}.
 *   <li>{@link Duration} - Defines the polling interval, i.e., the time between consecutive scans.
 * </ul>
 *
 * @param <OUT> The type of the records produced by this source.
 */
@PublicEvolving
public class KuduSource<OUT> implements Source<OUT, KuduSourceSplit, KuduSourceEnumeratorState> {
    private final KuduReaderConfig readerConfig;
    private final KuduTableInfo tableInfo;
    private final RowResultConverter<OUT> rowResultConverter;
    private final Duration period;

    private final Configuration configuration;

    KuduSource(
            KuduReaderConfig readerConfig,
            KuduTableInfo tableInfo,
            RowResultConverter<OUT> rowResultConverter,
            Duration period) {
        this.tableInfo = tableInfo;
        this.readerConfig = readerConfig;
        this.rowResultConverter = rowResultConverter;
        this.period = period;
        this.configuration = new Configuration();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<KuduSourceSplit, KuduSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<KuduSourceSplit> enumContext) {
        return new KuduSourceEnumerator(tableInfo, readerConfig, period, enumContext);
    }

    @Override
    public SplitEnumerator<KuduSourceSplit, KuduSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<KuduSourceSplit> enumContext,
            KuduSourceEnumeratorState checkpoint)
            throws Exception {
        return new KuduSourceEnumerator(tableInfo, readerConfig, period, enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<KuduSourceSplit> getSplitSerializer() {
        return new KuduSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<KuduSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new KuduSourceEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<OUT, KuduSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new KuduSourceReader(
                () -> new KuduSourceSplitReader(readerConfig),
                configuration,
                readerContext,
                rowResultConverter);
    }
}
