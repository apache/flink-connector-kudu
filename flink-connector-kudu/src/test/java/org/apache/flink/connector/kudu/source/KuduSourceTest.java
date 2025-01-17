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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.connector.kudu.connector.converter.RowResultRowConverter;
import org.apache.flink.connector.kudu.testutils.KuduSourceITBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link KuduSource}. */
public class KuduSourceTest extends KuduSourceTestBase implements KuduSourceITBase {
    private static Queue<Row> collectedRecords;

    @BeforeEach
    public void init() {
        super.init();
        collectedRecords = new ConcurrentLinkedDeque<>();
    }

    @Test
    void testNonExistentReaderConfig() {
        KuduSourceBuilder<Row> builder =
                new KuduSourceBuilder<Row>()
                        .setTableInfo(getTableInfo())
                        .setRowResultConverter(new RowResultRowConverter())
                        .setPeriod(Duration.ofSeconds(1));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Reader config");
    }

    @Test
    void testNonExistentTableInfo() {
        KuduSourceBuilder<Row> builder =
                new KuduSourceBuilder<Row>()
                        .setReaderConfig(getReaderConfig())
                        .setRowResultConverter(new RowResultRowConverter())
                        .setPeriod(Duration.ofSeconds(1));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table info");
    }

    @Test
    void testNonExistentRowResultConverter() {
        KuduSourceBuilder<Row> builder =
                new KuduSourceBuilder<Row>()
                        .setReaderConfig(getReaderConfig())
                        .setTableInfo(getTableInfo())
                        .setPeriod(Duration.ofSeconds(1));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RowResultConverter");
    }

    @Test
    void testNonExistentPeriod() {
        KuduSourceBuilder<Row> builder =
                new KuduSourceBuilder<Row>()
                        .setReaderConfig(getReaderConfig())
                        .setTableInfo(getTableInfo())
                        .setRowResultConverter(new RowResultRowConverter());

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Period");
    }

    @Test
    public void testRecordsFromSource(@InjectClusterClient ClusterClient<?> client)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KuduSource<Row> kuduSource =
                new KuduSourceBuilder<Row>()
                        .setReaderConfig(getReaderConfig())
                        .setTableInfo(getTableInfo())
                        .setRowResultConverter(new RowResultRowConverter())
                        .setPeriod(Duration.ofSeconds(1))
                        .build();

        env.fromSource(kuduSource, WatermarkStrategy.noWatermarks(), "KuduSource")
                .returns(TypeInformation.of(Row.class))
                .addSink(new TestingSinkFunction());

        JobID jobID = env.executeAsync().getJobID();
        waitExpectation(() -> collectedRecords.size() == 10);
        assertThat(collectedRecords.size()).isEqualTo(10);
        insertRows(10);
        waitExpectation(() -> collectedRecords.size() == 20);
        assertThat(collectedRecords.size()).isEqualTo(20);
        client.cancel(jobID);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void waitExpectation(Supplier<Boolean> condition) throws Exception {
        CompletableFuture<Void> future =
                CompletableFuture.runAsync(
                        () -> {
                            while (true) {
                                if (condition.get()) {
                                    break;
                                }
                                sleep(50);
                            }
                        });
        future.get();
    }

    /** A sink function to collect the records. */
    static class TestingSinkFunction implements SinkFunction<Row> {

        @Override
        public void invoke(Row value, Context context) throws Exception {
            collectedRecords.add(value);
        }
    }
}
