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

import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link KuduSplitGenerator}. */
public class KuduSplitGeneratorTest extends KuduSourceTestBase {

    @Test
    public void testGenerateGoodFullScanSplits() {
        KuduSplitGenerator generator = new KuduSplitGenerator(getReaderConfig(), getTableInfo());
        long now = getCurrentHybridTime();
        List<KuduSourceSplit> splits = generator.generateFullScanSplits(now);
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
    }

    @Test
    public void testGenerateGoodIncrementalSplits() throws Exception {
        KuduSplitGenerator generator = new KuduSplitGenerator(getReaderConfig(), getTableInfo());
        long startHT = getCurrentHybridTime();
        Thread.sleep(1000);
        long endHT = getCurrentHybridTime();
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
    }

    @Test
    public void testInvalidReaderConfig() {
        KuduReaderConfig invalidReaderConfig =
                KuduReaderConfig.Builder.setMasters("invalidMasters").build();
        KuduSplitGenerator generator = new KuduSplitGenerator(invalidReaderConfig, getTableInfo());

        long now = getCurrentHybridTime();
        Exception exception =
                assertThrows(
                        RuntimeException.class,
                        () -> {
                            List<KuduSourceSplit> splits = generator.generateFullScanSplits(now);
                        });
        assertThat(exception.getMessage()).contains("Error during full snapshot scan");
    }

    @Test
    public void testInvalidTableInfo() {
        KuduTableInfo invalidTableInfo = KuduTableInfo.forTable("nonExistentTable");
        KuduSplitGenerator generator = new KuduSplitGenerator(getReaderConfig(), invalidTableInfo);

        long now = getCurrentHybridTime();
        Exception exception =
                assertThrows(
                        RuntimeException.class,
                        () -> {
                            List<KuduSourceSplit> splits = generator.generateFullScanSplits(now);
                        });
        assertThat(exception.getMessage()).contains("Error during full snapshot scan");
    }
}
