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

package org.apache.flink.connector.kudu.connector.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.tuple.Tuple;

/** Logic to map a Flink Tuple to a Kudu-compatible format. */
@PublicEvolving
public class TupleOperationMapper<T extends Tuple> extends AbstractSingleOperationMapper<T> {

    protected TupleOperationMapper(String[] columnNames) {
        super(columnNames);
    }

    public TupleOperationMapper(String[] columnNames, KuduOperation operation) {
        super(columnNames, operation);
    }

    @Override
    public Object getField(T input, int i) {
        return input.getField(i);
    }
}
