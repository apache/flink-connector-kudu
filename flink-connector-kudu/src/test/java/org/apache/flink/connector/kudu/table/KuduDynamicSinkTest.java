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

import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.KuduTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Unit Tests for {@link org.apache.flink.connector.kudu.table.KuduDynamicTableSink}. */
public class KuduDynamicSinkTest extends KuduTestBase {
    public static final String INPUT_TABLE = "books";
    public static StreamExecutionEnvironment env;
    public static TableEnvironment tEnv;

    @BeforeEach
    public void init() {
        KuduTableInfo tableInfo = booksTableInfo(INPUT_TABLE, true);
        setUpDatabase(tableInfo);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterEach
    public void clean() {
        KuduTableInfo tableInfo = booksTableInfo(INPUT_TABLE, true);
        cleanDatabase(tableInfo);
    }

    @Test
    public void testKuduSink() {
        String createSql =
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id int PRIMARY KEY NOT ENFORCED,"
                        + "title string,"
                        + "author string,"
                        + "price double,"
                        + "quantity int"
                        + ") WITH ("
                        + "  'connector'='kudu',"
                        + "  'masters'='"
                        + 123245
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "','sink.max-buffer-size'='1024"
                        + "','sink.flush-interval'='1000ms"
                        + "','sink.operation-timeout'='500ms"
                        + "','sink.ignore-not-found'='true"
                        + "','sink.ignore-duplicate'='true'"
                        + ")";
        tEnv.executeSql(createSql);
        tEnv.executeSql(
                "insert into " + INPUT_TABLE + " values(1006,'test title','test author',10.1,10)");
        CloseableIterator<Row> collected =
                tEnv.executeSql("select * from " + INPUT_TABLE + " where id =1006").collect();
        assertNotNull(collected);
    }
}
