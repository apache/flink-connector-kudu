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

package org.apache.flink.connector.kudu.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.connector.kudu.source.split.SplitFinishedEvent;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The Kudu source reader.
 *
 * @param <OUT> the type of records read from the source.
 */
public class KuduSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<OUT, OUT, KuduSourceSplit, KuduSourceSplit> {

    public KuduSourceReader(
            Supplier<SplitReader<OUT, KuduSourceSplit>> splitReaderSupplier,
            Configuration config,
            SourceReaderContext context,
            RowResultConverter rowResultConverter) {
        super(splitReaderSupplier, new KuduRecordEmitter<>(rowResultConverter), config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, KuduSourceSplit> finishedSplits) {
        context.sendSourceEventToCoordinator(
                new SplitFinishedEvent(new ArrayList<>(finishedSplits.values())));
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected KuduSourceSplit initializedState(KuduSourceSplit split) {
        return split;
    }

    @Override
    protected KuduSourceSplit toSplitType(String splitId, KuduSourceSplit splitState) {
        return splitState;
    }
}
