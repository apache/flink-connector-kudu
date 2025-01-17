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

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** The Kudu source reader that reads data for corresponding splits. */
public class KuduSourceSplitReader implements SplitReader<RowResult, KuduSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(KuduSourceSplitReader.class);
    private final KuduClient kuduClient;
    private final List<KuduSourceSplit> splits;
    private final AtomicBoolean wakeUpFlag = new AtomicBoolean(false);

    public KuduSourceSplitReader(KuduReaderConfig readerConfig) {
        this.kuduClient = new KuduClient.KuduClientBuilder(readerConfig.getMasters()).build();
        this.splits = new ArrayList<>();
    }

    @Override
    public RecordsWithSplitIds<RowResult> fetch() throws IOException {
        wakeUpFlag.compareAndSet(true, false);

        final Optional<KuduSourceSplit> currentSplitOpt = getNextSplit();
        if (!currentSplitOpt.isPresent()) {
            return new RecordsBySplits.Builder<RowResult>().build();
        }

        KuduSourceSplit currentSplit = currentSplitOpt.get();
        byte[] serializedToken = currentSplit.getSerializedScanToken();
        KuduScanner scanner = KuduScanToken.deserializeIntoScanner(serializedToken, kuduClient);
        RecordsBySplits.Builder<RowResult> builder = new RecordsBySplits.Builder<>();

        try {
            while (scanner.hasMoreRows()) {
                for (RowResult row : scanner.nextRows()) {
                    if (wakeUpFlag.get()) {
                        LOG.debug("Wakeup signal received inside row iteration, stopping fetch.");
                        scanner.close(); // Close the scanner
                        splits.add(currentSplit); // Put the split back
                        return new RecordsBySplits.Builder<RowResult>()
                                .build(); // Return empty result
                    }
                    builder.add(currentSplit.splitId(), row);
                }
            }
            builder.addFinishedSplit(
                    currentSplit.splitId()); // Mark split as completed only after the loop

        } finally {
            scanner.close(); // Ensure scanner is always closed
        }

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KuduSourceSplit> splitsChanges) {
        LOG.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {
        LOG.debug("Wakeup called, setting flag.");
        wakeUpFlag.set(true);
    }

    @Override
    public void close() throws Exception {
        kuduClient.close();
    }

    private Optional<KuduSourceSplit> getNextSplit() {
        if (splits.isEmpty()) {
            return Optional.empty();
        }
        Iterator<KuduSourceSplit> iterator = splits.iterator();
        KuduSourceSplit next = iterator.next();
        iterator.remove();
        return Optional.of(next);
    }
}
