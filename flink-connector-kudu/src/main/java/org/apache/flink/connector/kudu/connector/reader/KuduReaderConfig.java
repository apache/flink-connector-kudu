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

package org.apache.flink.connector.kudu.connector.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.kudu.format.KuduRowInputFormat;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.kudu.client.ReplicaSelection;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration used by {@link KuduRowInputFormat}. Specifies connection and other necessary
 * properties.
 */
@PublicEvolving
public class KuduReaderConfig implements Serializable {

    private final String masters;
    private final int rowLimit;
    private final long splitSizeBytes;
    private final int batchSizeBytes;
    private final long scanRequestTimeout;
    private final boolean prefetching;
    private final long keepAlivePeriodMs;
    private final ReplicaSelection replicaSelection;

    private KuduReaderConfig(
            String masters,
            int rowLimit,
            long splitSizeBytes,
            int batchSizeBytes,
            long scanRequestTimeout,
            boolean prefetching,
            long keepAlivePeriodMs,
            ReplicaSelection replicaSelection) {

        this.masters = checkNotNull(masters, "Kudu masters cannot be null");
        this.rowLimit = checkNotNull(rowLimit, "Kudu rowLimit cannot be null");
        this.splitSizeBytes = checkNotNull(splitSizeBytes, "Kudu split size cannot be null");
        this.batchSizeBytes = checkNotNull(batchSizeBytes, "Kudu batch size cannot be null");
        this.scanRequestTimeout =
                checkNotNull(scanRequestTimeout, "Kudu scan request timeout cannot be null");
        this.prefetching = checkNotNull(prefetching, "Kudu prefetching cannot be null");
        this.keepAlivePeriodMs =
                checkNotNull(keepAlivePeriodMs, "Kudu keep alive period ms cannot be null");
        this.replicaSelection =
                checkNotNull(replicaSelection, "Kudu replica selection cannot be null");
    }

    public String getMasters() {
        return masters;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public long getSplitSizeBytes() {
        return splitSizeBytes;
    }

    public int getBatchSizeBytes() {
        return batchSizeBytes;
    }

    public long getScanRequestTimeout() {
        return scanRequestTimeout;
    }

    public boolean isPrefetching() {
        return prefetching;
    }

    public long getKeepAlivePeriodMs() {
        return keepAlivePeriodMs;
    }

    public ReplicaSelection getReplicaSelection() {
        return replicaSelection;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masters", masters)
                .append("rowLimit", rowLimit)
                .append("splitSizeBytes", splitSizeBytes)
                .append("batchSizeBytes", batchSizeBytes)
                .append("scanRequestTimeout", scanRequestTimeout)
                .append("prefetching", prefetching)
                .append("keepAlivePeriodMs", keepAlivePeriodMs)
                .append("replicaSelection", replicaSelection)
                .toString();
    }

    /** Builder for the {@link KuduReaderConfig}. */
    public static class Builder {
        // Reference from AbstractKuduScannerBuilder limit Long.MAX_VALUE
        private static final int DEFAULT_ROW_LIMIT = Integer.MAX_VALUE;

        private final String masters;
        private final int rowLimit;
        // Reference from KuduScanTokenBuilder splitSizeBytes DEFAULT_SPLIT_SIZE_BYTES -1
        private long splitSizeBytes = -1;
        // Reference from BackupOptions DefaultScanBatchSize 1024 * 1024 * 20
        private int batchSizeBytes = 1024 * 1024 * 20; // 20 MiB
        // Reference from AsyncKuduClient DEFAULT_OPERATION_TIMEOUT_MS 30000
        private long scanRequestTimeout = 30000;
        // Reference from BackupOptions DefaultScanPrefetching false
        private boolean prefetching = false;
        // Reference from AsyncKuduClient DEFAULT_KEEP_ALIVE_PERIOD_MS 15000
        private long keepAlivePeriodMs = 15000;
        // Reference from BackupOptions DefaultScanLeaderOnly false
        private ReplicaSelection replicaSelection = ReplicaSelection.CLOSEST_REPLICA;

        private Builder(String masters) {
            this(masters, DEFAULT_ROW_LIMIT);
        }

        private Builder(String masters, int rowLimit) {
            this.masters = masters;
            this.rowLimit = rowLimit;
        }

        public static Builder setMasters(String masters) {
            return new Builder(masters);
        }

        public Builder setRowLimit(int rowLimit) {
            return new Builder(masters, rowLimit);
        }

        public Builder setSplitSizeBytes(long splitSizeBytes) {
            this.splitSizeBytes = splitSizeBytes;
            return this;
        }

        public Builder setBatchSizeBytes(int batchSizeBytes) {
            this.batchSizeBytes = batchSizeBytes;
            return this;
        }

        public Builder setScanRequestTimeout(long scanRequestTimeout) {
            this.scanRequestTimeout = scanRequestTimeout;
            return this;
        }

        public Builder setPrefetching(boolean prefetching) {
            this.prefetching = prefetching;
            return this;
        }

        public Builder setKeepAlivePeriodMs(long keepAlivePeriodMs) {
            this.keepAlivePeriodMs = keepAlivePeriodMs;
            return this;
        }

        public Builder setReplicaSelection(ReplicaSelection replicaSelection) {
            this.replicaSelection = replicaSelection;
            return this;
        }

        public KuduReaderConfig build() {
            return new KuduReaderConfig(
                    masters,
                    rowLimit,
                    splitSizeBytes,
                    batchSizeBytes,
                    scanRequestTimeout,
                    prefetching,
                    keepAlivePeriodMs,
                    replicaSelection);
        }
    }
}
