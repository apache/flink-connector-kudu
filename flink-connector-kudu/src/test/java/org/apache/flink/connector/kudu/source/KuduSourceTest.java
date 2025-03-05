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
import org.apache.flink.connector.kudu.connector.converter.RowResultRowConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KuduSource}. */
public class KuduSourceTest {
    @Test
    void testNonExistentReaderConfig() {
        KuduSourceBuilder<Row> builder =
                new KuduSourceBuilder<Row>()
                        .setTableInfo(KuduTableInfo.forTable("table"))
                        .setRowResultConverter(new RowResultRowConverter());

        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Reader config");
    }

    @Test
    void testNonExistentTableInfo() {
        KuduSourceBuilder<Row> builder =
                new KuduSourceBuilder<Row>()
                        .setReaderConfig(KuduReaderConfig.Builder.setMasters("masters").build())
                        .setRowResultConverter(new RowResultRowConverter());

        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Table info");
    }

    @Test
    void testNonExistentRowResultConverter() {
        KuduSourceBuilder<Row> builder =
                new KuduSourceBuilder<Row>()
                        .setReaderConfig(KuduReaderConfig.Builder.setMasters("masters").build())
                        .setTableInfo(KuduTableInfo.forTable("table"));

        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("RowResultConverter");
    }

    @Test
    void testNonExistentDiscoveryIntervalContinuousMode() {
        KuduSourceBuilder<Row> builder =
                new KuduSourceBuilder<Row>()
                        .setReaderConfig(KuduReaderConfig.Builder.setMasters("masters").build())
                        .setTableInfo(KuduTableInfo.forTable("table"))
                        .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                        .setRowResultConverter(new RowResultRowConverter());

        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Discovery period");
    }
}
