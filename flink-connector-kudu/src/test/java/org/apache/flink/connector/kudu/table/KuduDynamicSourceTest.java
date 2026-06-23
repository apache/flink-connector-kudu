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
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Unit Tests for {@link org.apache.flink.connector.kudu.table.KuduDynamicTableSource}. */
public class KuduDynamicSourceTest extends KuduTestBase {
    private StreamExecutionEnvironment env;
    private TableEnvironment tEnv;
    private KuduTableInfo tableInfo;

    @BeforeEach
    public void init() {
        tableInfo = uniqueBooksTableInfo(true);
        setUpDatabase(tableInfo);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterEach
    public void clean() {
        if (tableInfo != null) {
            cleanDatabase(tableInfo);
            tableInfo = null;
        }
    }

    @Test
    public void testKuduSource() throws Exception {
        // "id", "title", "author", "price", "quantity"
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableInfo.getName()
                        + "("
                        + "id int PRIMARY KEY NOT ENFORCED,"
                        + "title string,"
                        + "author string,"
                        + "price double,"
                        + "quantity int"
                        + ") WITH ("
                        + "  'connector'='kudu',"
                        + "  'masters'='"
                        + getMasterAddress()
                        + "',"
                        + "  'table-name'='"
                        + tableInfo.getName()
                        + "',"
                        + "'scan.row-size'='10'"
                        + ")");

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + tableInfo.getName()).collect();
        assertNotNull(collected);
    }

    @Test
    public void testProject() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableInfo.getName()
                        + "("
                        + "id int PRIMARY KEY NOT ENFORCED,"
                        + "title string,"
                        + "author string,"
                        + "price double,"
                        + "quantity int"
                        + ") WITH ("
                        + "  'connector'='kudu',"
                        + "  'masters'='"
                        + getMasterAddress()
                        + "',"
                        + "  'table-name'='"
                        + tableInfo.getName()
                        + "',"
                        + "'scan.row-size'='10'"
                        + ")");

        Iterator<Row> collected =
                tEnv.executeSql("SELECT id,title,author FROM " + tableInfo.getName()).collect();
        assertNotNull(collected);
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1001, Java for dummies, Tan Ah Teck]",
                                "+I[1002, More Java for dummies, Tan Ah Teck]",
                                "+I[1003, More Java for more dummies, Mohammad Ali]",
                                "+I[1004, A Cup of Java, Kumar]",
                                "+I[1005, A Teaspoon of Java, Kevin Jones]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    @Test
    public void testLimit() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableInfo.getName()
                        + "("
                        + "id int PRIMARY KEY NOT ENFORCED,"
                        + "title string,"
                        + "author string,"
                        + "price double,"
                        + "quantity int"
                        + ") WITH ("
                        + "  'connector'='kudu',"
                        + "  'masters'='"
                        + getMasterAddress()
                        + "',"
                        + "  'table-name'='"
                        + tableInfo.getName()
                        + "',"
                        + "'scan.row-size'='10'"
                        + ")");

        Iterator<Row> collected =
                tEnv.executeSql("SELECT * FROM " + tableInfo.getName() + " LIMIT 1").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(1, result.size());
    }

    @Test
    public void testLookupJoin() {
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableInfo.getName()
                        + "("
                        + "id int PRIMARY KEY NOT ENFORCED,"
                        + "title string,"
                        + "author string,"
                        + "price double,"
                        + "quantity int"
                        + ") WITH ("
                        + "  'connector'='kudu',"
                        + "  'masters'='"
                        + getMasterAddress()
                        + "',"
                        + "  'table-name'='"
                        + tableInfo.getName()
                        + "',"
                        + "'scan.row-size'='10'"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE datagen"
                        + "("
                        + "id int,"
                        + "isbn string,"
                        + "proctime as PROCTIME()"
                        + ") WITH ("
                        + "  'connector'='datagen',"
                        + "  'number-of-rows'='5',"
                        + "  'fields.id.kind'='sequence',"
                        + "  'fields.isbn.kind'='sequence',"
                        + "  'fields.id.start'='1001',"
                        + "  'fields.isbn.start'='1',"
                        + "  'fields.id.end'='1005',"
                        + "  'fields.isbn.end'='5'"
                        + ")");

        Iterator<Row> collected =
                tEnv.executeSql(
                                "SELECT d.id, isbn, title FROM datagen as d"
                                        + " JOIN "
                                        + tableInfo.getName()
                                        + " FOR SYSTEM_TIME AS OF d.proctime AS k"
                                        + " ON d.id=k.id"
                                        + " WHERE k.title='Java for dummies'")
                        .collect();
        assertNotNull(collected);
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(1, result.size());
        assertEquals("+I[1001, 1, Java for dummies]", result.get(0));
    }
}
