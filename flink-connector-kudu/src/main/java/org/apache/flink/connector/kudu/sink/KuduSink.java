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

package org.apache.flink.connector.kudu.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.failure.KuduFailureHandler;
import org.apache.flink.connector.kudu.connector.writer.KuduOperationMapper;
import org.apache.flink.connector.kudu.connector.writer.KuduWriter;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;

import java.io.IOException;

/**
 * Streaming Sink that executes Kudu operations based on the incoming elements. The target Kudu
 * table is defined in the {@link KuduTableInfo} object together with parameters for table creation
 * in case the table does not exist.
 *
 * <p>Incoming records are mapped to Kudu table operations using the provided {@link
 * KuduOperationMapper} logic. While failures resulting from the operations are handled by the
 * {@link KuduFailureHandler} instance.
 *
 * @param <IN> type of the input records written to Kudu
 */
@PublicEvolving
public class KuduSink<IN> implements Sink<IN> {

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduOperationMapper<IN> operationMapper;
    private final KuduFailureHandler failureHandler;

    KuduSink(
            KuduTableInfo tableInfo,
            KuduWriterConfig writerConfig,
            KuduOperationMapper<IN> operationMapper,
            KuduFailureHandler failureHandler) {
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.operationMapper = operationMapper;
        this.failureHandler = failureHandler;
    }

    public static <IN> KuduSinkBuilder<IN> builder() {
        return new KuduSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext initContext) throws IOException {
        return new KuduWriter<>(tableInfo, writerConfig, operationMapper, failureHandler);
    }
}
