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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.connector.kudu.connector.converter.RowResultRowConverter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** IT cases for using Kudu Source. */
public class KuduSourceITCase extends KuduSourceTestBase {
    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    public static StreamExecutionEnvironment env;

    private static Queue<Row> collectedRecords;

    @BeforeEach
    public void init() throws Exception {
        super.init();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        collectedRecords = new ConcurrentLinkedDeque<>();
    }

    @Test
    public void testRecordsFromSourceUnbounded(@InjectClusterClient ClusterClient<?> client)
            throws Exception {

        KuduSource<Row> kuduSource =
                new KuduSourceBuilder<Row>()
                        .setReaderConfig(getReaderConfig())
                        .setTableInfo(getTableInfo())
                        .setRowResultConverter(new RowResultRowConverter())
                        .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                        .setDiscoveryPeriod(Duration.ofSeconds(1))
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

    @Test
    public void testRecordsFromSourceBounded() throws Exception {

        KuduSource<Row> kuduSource =
                new KuduSourceBuilder<Row>()
                        .setReaderConfig(getReaderConfig())
                        .setTableInfo(getTableInfo())
                        .setRowResultConverter(new RowResultRowConverter())
                        .build();

        env.fromSource(kuduSource, WatermarkStrategy.noWatermarks(), "KuduSource")
                .returns(TypeInformation.of(Row.class))
                .addSink(new TestingSinkFunction());

        env.execute();
        assertThat(collectedRecords.size()).isEqualTo(10);
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
