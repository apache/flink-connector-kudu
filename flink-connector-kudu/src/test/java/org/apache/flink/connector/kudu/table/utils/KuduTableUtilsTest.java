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

package org.apache.flink.connector.kudu.table.utils;

import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.util.InstantiationUtil;

import org.apache.kudu.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KuduTableUtils}. */
class KuduTableUtilsTest {

    @Test
    void testCreateTableInfoIsSerializableWithoutLambdas() throws Exception {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", DataTypes.BIGINT().notNull()),
                                Column.physical("name", DataTypes.STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("PK_id", Collections.singletonList("id")));
        Map<String, String> props = new HashMap<>();
        props.put("hash-columns", "id");
        props.put("replicas", "1");

        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo("test_table", schema, props);
        byte[] bytes = InstantiationUtil.serializeObject(tableInfo);

        // Serializable lambdas fail to deserialize once org.apache.kudu is relocated by the
        // flink-sql-connector-kudu shading, so the factories must be regular classes.
        assertThat(new String(bytes, StandardCharsets.ISO_8859_1))
                .doesNotContain("java.lang.invoke.SerializedLambda");

        KuduTableInfo restored =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        assertThat(restored.getCreateTableIfNotExists()).isTrue();
        assertThat(restored.getCreateTableOptions()).isNotNull();

        Schema kuduSchema = restored.getSchema();
        assertThat(kuduSchema.getColumnCount()).isEqualTo(2);
        assertThat(kuduSchema.getPrimaryKeyColumns()).hasSize(1);
        assertThat(kuduSchema.getPrimaryKeyColumns().get(0).getName()).isEqualTo("id");
    }
}
