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
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.failure.DefaultKuduFailureHandler;
import org.apache.flink.connector.kudu.connector.failure.KuduFailureHandler;
import org.apache.flink.connector.kudu.connector.writer.KuduOperationMapper;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Builder to construct {@link KuduSink}.
 *
 * @param <IN> type of the input records written to Kudu
 */
@PublicEvolving
public class KuduSinkBuilder<IN> {

    private KuduTableInfo tableInfo;
    private KuduWriterConfig writerConfig;
    private KuduOperationMapper<IN> operationMapper;
    private KuduFailureHandler failureHandler = new DefaultKuduFailureHandler();

    public KuduSinkBuilder<IN> setTableInfo(KuduTableInfo tableInfo) {
        this.tableInfo = tableInfo;
        return this;
    }

    public KuduSinkBuilder<IN> setWriterConfig(KuduWriterConfig writerConfig) {
        this.writerConfig = writerConfig;
        return this;
    }

    public KuduSinkBuilder<IN> setOperationMapper(KuduOperationMapper<IN> operationMapper) {
        this.operationMapper = operationMapper;
        return this;
    }

    public KuduSinkBuilder<IN> setFailureHandler(KuduFailureHandler failureHandler) {
        this.failureHandler = failureHandler;
        return this;
    }

    public KuduSink<IN> build() {
        checkArgument(tableInfo != null, "Table info must be provided.");
        checkArgument(writerConfig != null, "Writer config must be provided.");
        checkArgument(operationMapper != null, "Operation mapper must be provided.");

        if (failureHandler == null) {
            failureHandler = new DefaultKuduFailureHandler();
        }

        return new KuduSink<>(tableInfo, writerConfig, operationMapper, failureHandler);
    }
}
