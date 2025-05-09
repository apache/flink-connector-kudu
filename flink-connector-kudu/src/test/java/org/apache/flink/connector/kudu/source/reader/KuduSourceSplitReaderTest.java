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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.kudu.source.KuduSourceTestBase;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.connector.kudu.source.utils.KuduSourceUtils;
import org.apache.flink.connector.kudu.source.utils.KuduSplitGenerator;

import org.apache.kudu.client.RowResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KuduSourceSplitReader}. */
public class KuduSourceSplitReaderTest extends KuduSourceTestBase {

    private List<KuduSourceSplit> generateSplits() {
        List<KuduSourceSplit> splits;

        try (KuduSplitGenerator generator =
                new KuduSplitGenerator(getReaderConfig(), getTableInfo())) {
            long now = KuduSourceUtils.getCurrentHybridTime();
            splits = generator.generateFullScanSplits(now);
        } catch (Exception e) {
            throw new RuntimeException("Failed to close KuduSplitGenerator", e);
        }
        return splits;
    }

    @Test
    public void testBasicRecordFetching() {
        int recordsFetched = 0;

        try (KuduSourceSplitReader splitReader = new KuduSourceSplitReader(getReaderConfig())) {
            List<KuduSourceSplit> splits = generateSplits();
            for (KuduSourceSplit split : splits) {
                splitReader.handleSplitsChanges(
                        new SplitsAddition<>(new ArrayList<>(Collections.singletonList(split))));
                RecordsWithSplitIds<RowResult> fetchedRecordsWithSplitIds = splitReader.fetch();
                assertThat(fetchedRecordsWithSplitIds.nextSplit()).isNotNull();

                List<RowResult> records = new ArrayList<>();
                RowResult nextRecordFromSplit = fetchedRecordsWithSplitIds.nextRecordFromSplit();
                while (nextRecordFromSplit != null) {
                    records.add(nextRecordFromSplit);
                    nextRecordFromSplit = fetchedRecordsWithSplitIds.nextRecordFromSplit();
                }

                assertThat(records.size()).isGreaterThan(0);
                recordsFetched += records.size();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to close KuduSourceSplitReader", e);
        }

        assertThat(recordsFetched).isEqualTo(getTestRowsCount());
    }
}
