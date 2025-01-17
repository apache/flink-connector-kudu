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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.connector.kudu.source.split.SplitFinishedEvent;

import org.apache.kudu.util.HybridTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The Kudu source enumerator is responsible for periodically discovering and assigning new splits
 * when possible.
 *
 * <p>To provide CDC-like functionality, the enumeration works as follows: Initially, we perform a
 * snapshot read of the table and mark the snapshot time as t0. From that point onward, we perform
 * differential scans in the time intervals t0 - t1, t1 - t2, and so on.
 *
 * <p>This approach means that new splits can only be enumerated once the current time range is
 * fully processed.
 *
 * <p>The process is controlled as follows: Once a set of splits is enumerated for a time range, we
 * track: Unassigned splits that have been discovered but not yet assigned to readers. Pending
 * splits that are assigned but not yet fully processed. A new set of splits is generated only when
 * there are no remaining unassigned or pending splits.
 */
public class KuduSourceEnumerator
        implements SplitEnumerator<KuduSourceSplit, KuduSourceEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(KuduSourceEnumerator.class);

    private final SplitEnumeratorContext<KuduSourceSplit> context;
    private final List<Integer> readersAwaitingSplit;
    private final List<KuduSourceSplit> unassigned;
    private final List<KuduSourceSplit> pending;
    private final Duration period;
    private final KuduSplitGenerator splitGenerator;
    private long lastEndTimestamp;

    public KuduSourceEnumerator(
            KuduTableInfo tableInfo,
            KuduReaderConfig readerConfig,
            Duration period,
            SplitEnumeratorContext<KuduSourceSplit> context,
            KuduSourceEnumeratorState enumState) {
        this.period = period;
        this.context = context;
        this.readersAwaitingSplit = new ArrayList<>();
        this.unassigned = enumState == null ? new ArrayList<>() : enumState.getUnassigned();
        this.pending = enumState == null ? new ArrayList<>() : enumState.getPending();
        this.splitGenerator = new KuduSplitGenerator(readerConfig, tableInfo);
        this.lastEndTimestamp = enumState == null ? -1L : enumState.getLastEndTimestamp();
    }

    @Override
    public void start() {
        context.callAsync(
                () -> this.enumerateNewSplits(() -> pending.isEmpty() && unassigned.isEmpty()),
                this::assignSplits,
                0,
                this.period.toMillis());
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        readersAwaitingSplit.add(subtaskId);
        assignSplitsToReaders();
    }

    @Override
    public void addSplitsBack(List<KuduSourceSplit> splits, int subtaskId) {
        LOG.debug("File Source Enumerator adds splits back: {}", splits);
        unassigned.addAll(splits);
        if (context.registeredReaders().containsKey(subtaskId)) {
            readersAwaitingSplit.add(subtaskId);
        }
        assignSplitsToReaders();
    }

    @Override
    public void addReader(int subtaskId) {
        // The source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public KuduSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new KuduSourceEnumeratorState(lastEndTimestamp, unassigned, pending);
    }

    @Override
    public void close() throws IOException {
        try {
            splitGenerator.close();
        } catch (Exception e) {
            throw new IOException("Error closing split generator", e);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SplitFinishedEvent) {
            SplitFinishedEvent splitFinishedEvent = (SplitFinishedEvent) sourceEvent;
            LOG.debug(
                    "Received SplitFinishedEvent from subtask {} for splits: {}",
                    subtaskId,
                    splitFinishedEvent.getFinishedSplits());
            pending.removeAll(splitFinishedEvent.getFinishedSplits());
            readersAwaitingSplit.add(subtaskId);
        }
    }

    // This function is invoked repeatedly according to this.period if there are no outstanding
    // splits.
    // Outstanding meaning that no pending splits, and no enumerated but not assigned splits for the
    // current period.
    private List<KuduSourceSplit> enumerateNewSplits(@Nonnull Supplier<Boolean> shouldGenerate) {
        if (shouldGenerate.get() == Boolean.FALSE) {
            return null;
        }
        List<KuduSourceSplit> newSplits;

        if (isFirstSplitGeneration()) {
            this.lastEndTimestamp = getCurrentHybridTime();
            newSplits = this.splitGenerator.generateFullScanSplits(this.lastEndTimestamp);
        } else {
            long startHT = this.lastEndTimestamp;
            long endHT = getCurrentHybridTime();
            newSplits = this.splitGenerator.generateIncrementalSplits(startHT, endHT);
            this.lastEndTimestamp = endHT;
        }

        return newSplits;
    }

    private void assignSplits(List<KuduSourceSplit> splits, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate Kudu splits.", error);
            return;
        }

        if (splits != null) {
            this.unassigned.addAll(splits);
        }
        assignSplitsToReaders();
    }

    private void assignSplitsToReaders() {
        final Iterator<Integer> awaitingSubtasks = readersAwaitingSplit.iterator();

        while (awaitingSubtasks.hasNext()) {
            final int awaitingSubtask = awaitingSubtasks.next();

            // If the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(awaitingSubtask)) {
                awaitingSubtasks.remove();
                continue;
            }

            final Optional<KuduSourceSplit> nextSplit = getNextSplit();
            if (nextSplit.isPresent()) {
                context.assignSplit(nextSplit.get(), awaitingSubtask);
                awaitingSubtasks.remove();
                pending.add(nextSplit.get());
            } else {
                break;
            }
        }
    }

    private Optional<KuduSourceSplit> getNextSplit() {
        if (unassigned.isEmpty()) {
            return Optional.empty();
        }
        Iterator<KuduSourceSplit> iterator = unassigned.iterator();
        KuduSourceSplit next = iterator.next();
        iterator.remove();
        return Optional.of(next);
    }

    // Helper method to get the current Hybrid Time
    private long getCurrentHybridTime() {
        long fromMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        return HybridTimeUtil.physicalAndLogicalToHTTimestamp(fromMicros, 0) + 1;
    }

    private Boolean isFirstSplitGeneration() {
        return this.lastEndTimestamp == -1L;
    }
}
