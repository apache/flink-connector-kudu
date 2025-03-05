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
import org.apache.flink.connector.kudu.source.KuduSourceTestBase;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.connector.kudu.source.utils.KuduSourceUtils;
import org.apache.flink.connector.kudu.source.utils.KuduSplitGenerator;

import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KuduSplitGenerator}. */
public class KuduSplitGeneratorTest extends KuduSourceTestBase {

    private final long startHT;
    private final long endHT;
    private final long currentHT;

    public KuduSplitGeneratorTest() throws Exception {
        currentHT = KuduSourceUtils.getCurrentHybridTime();
        startHT = KuduSourceUtils.getCurrentHybridTime();
        Thread.sleep(1000);
        endHT = KuduSourceUtils.getCurrentHybridTime();
    }

    @Test
    public void testGenerateGoodFullScanSplits() {
        try (KuduSplitGenerator generator =
                new KuduSplitGenerator(getReaderConfig(), getTableInfo())) {
            List<KuduSourceSplit> splits = generator.generateFullScanSplits(currentHT);
            assertThat(splits.size()).isGreaterThan(0);
            // Check that all the splits can be properly deserialized into Kudu scanners.
            for (KuduSourceSplit split : splits) {
                try {
                    KuduScanner scanner =
                            KuduScanToken.deserializeIntoScanner(
                                    split.getSerializedScanToken(), getClient());
                } catch (Exception e) {
                    Assertions.fail("Deserialization failed: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to close KuduSplitGenerator", e);
        }
    }

    @Test
    public void testGenerateGoodIncrementalSplits() {
        try (KuduSplitGenerator generator =
                new KuduSplitGenerator(getReaderConfig(), getTableInfo())) {
            List<KuduSourceSplit> splits = generator.generateIncrementalSplits(startHT, endHT);
            assertThat(splits.size()).isGreaterThan(0);
            // Check that all the splits can be properly deserialized into Kudu scanners.
            for (KuduSourceSplit split : splits) {
                try {
                    KuduScanner scanner =
                            KuduScanToken.deserializeIntoScanner(
                                    split.getSerializedScanToken(), getClient());
                } catch (Exception e) {
                    Assertions.fail("Deserialization failed: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to close KuduSplitGenerator", e);
        }
    }

    @Test
    public void testInvalidReaderConfigFullScan() {
        KuduReaderConfig invalidReaderConfig =
                KuduReaderConfig.Builder.setMasters("invalidMasters").build();
        try (KuduSplitGenerator generator =
                new KuduSplitGenerator(invalidReaderConfig, getTableInfo())) {

            assertThatThrownBy(() -> generator.generateFullScanSplits(currentHT))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining(
                            "Error during full snapshot scan: Couldn't find a valid master");
        } catch (Exception e) {
            throw new RuntimeException("Failed to close KuduSplitGenerator", e);
        }
    }

    @Test
    public void testInvalidTableInfoFullScan() {
        KuduTableInfo invalidTableInfo = KuduTableInfo.forTable("nonExistentTable");
        try (KuduSplitGenerator generator =
                new KuduSplitGenerator(getReaderConfig(), invalidTableInfo)) {

            assertThatThrownBy(() -> generator.generateFullScanSplits(currentHT))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining(
                            "Error during full snapshot scan: the table does not exist");
        } catch (Exception e) {
            throw new RuntimeException("Failed to close KuduSplitGenerator", e);
        }
    }

    @Test
    public void testInvalidReaderConfigIncrementalScan() {
        KuduReaderConfig invalidReaderConfig =
                KuduReaderConfig.Builder.setMasters("invalidMasters").build();
        try (KuduSplitGenerator generator =
                new KuduSplitGenerator(invalidReaderConfig, getTableInfo())) {

            assertThatThrownBy(() -> generator.generateIncrementalSplits(startHT, endHT))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining(
                            "Error during incremental diff scan: Couldn't find a valid master");
        } catch (Exception e) {
            throw new RuntimeException("Failed to close KuduSplitGenerator", e);
        }
    }

    @Test
    public void testInvalidTableInfoIncrementalScan() {
        KuduTableInfo invalidTableInfo = KuduTableInfo.forTable("nonExistentTable");
        try (KuduSplitGenerator generator =
                new KuduSplitGenerator(getReaderConfig(), invalidTableInfo)) {

            assertThatThrownBy(() -> generator.generateIncrementalSplits(startHT, endHT))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining(
                            "Error during incremental diff scan: the table does not exist");
        } catch (Exception e) {
            throw new RuntimeException("Failed to close KuduSplitGenerator", e);
        }
    }
}
