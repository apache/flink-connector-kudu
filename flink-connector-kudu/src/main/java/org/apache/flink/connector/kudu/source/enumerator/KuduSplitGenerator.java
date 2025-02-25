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

package org.apache.flink.connector.kudu.source.enumerator;

import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;

import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;

import java.util.ArrayList;
import java.util.List;

/**
 * The class responsible for producing scan tokens for given timestamps and returning them in the
 * form of {@link KuduSourceSplit}.
 */
public class KuduSplitGenerator {
    private final KuduTableInfo tableInfo;
    private final KuduClient kuduClient;

    public KuduSplitGenerator(KuduReaderConfig readerConfig, KuduTableInfo tableInfo) {
        this.tableInfo = tableInfo;
        this.kuduClient = new KuduClient.KuduClientBuilder(readerConfig.getMasters()).build();
    }

    public List<KuduSourceSplit> generateFullScanSplits(long snapshotTimestamp) {
        if (snapshotTimestamp <= 0) {
            throw new IllegalArgumentException(
                    "Snapshot timestamp must be greater than 0, but was: " + snapshotTimestamp);
        }
        try {
            KuduTable table = kuduClient.openTable(tableInfo.getName());
            List<KuduScanToken> tokens =
                    kuduClient
                            .newScanTokenBuilder(table)
                            .snapshotTimestampRaw(snapshotTimestamp)
                            .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)
                            .build();

            return serializeTokens(tokens);
        } catch (Exception e) {
            throw new RuntimeException("Error during full snapshot scan: " + e.getMessage(), e);
        }
    }

    public List<KuduSourceSplit> generateIncrementalSplits(long startHT, long endHT) {
        if (startHT <= 0 || endHT <= 0) {
            throw new IllegalArgumentException(
                    "Start and end timestamps must be greater than 0. Given startHT: "
                            + startHT
                            + ", endHT: "
                            + endHT);
        }

        if (startHT >= endHT) {
            throw new IllegalArgumentException(
                    "Start timestamp must be less than end timestamp. Given startHT: "
                            + startHT
                            + ", endHT: "
                            + endHT);
        }

        try {
            KuduTable table = kuduClient.openTable(tableInfo.getName());
            List<KuduScanToken> tokens =
                    kuduClient.newScanTokenBuilder(table).diffScan(startHT, endHT).build();

            return serializeTokens(tokens);
        } catch (Exception e) {
            throw new RuntimeException("Error during incremental diff scan: " + e.getMessage(), e);
        }
    }

    private List<KuduSourceSplit> serializeTokens(List<KuduScanToken> tokens) {
        try {
            List<KuduSourceSplit> splits = new ArrayList<>();
            for (KuduScanToken token : tokens) {
                splits.add(new KuduSourceSplit(token.serialize()));
            }
            return splits;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error during source split serialization: " + e.getMessage(), e);
        }
    }

    public void close() throws Exception {
        kuduClient.close();
    }
}
