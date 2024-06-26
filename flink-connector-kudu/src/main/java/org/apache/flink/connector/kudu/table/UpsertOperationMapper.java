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

package org.apache.flink.connector.kudu.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.types.Row;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import java.util.Optional;

/** Logic to map a Flink DataStream upsert record to a Kudu-compatible format. */
@Internal
public class UpsertOperationMapper extends AbstractSingleOperationMapper<Tuple2<Boolean, Row>> {

    public UpsertOperationMapper(String[] columnNames) {
        super(columnNames);
    }

    @Override
    public Object getField(Tuple2<Boolean, Row> input, int i) {
        return input.f1.getField(i);
    }

    @Override
    public Optional<Operation> createBaseOperation(Tuple2<Boolean, Row> input, KuduTable table) {
        return Optional.of(input.f0 ? table.newUpsert() : table.newDelete());
    }
}
