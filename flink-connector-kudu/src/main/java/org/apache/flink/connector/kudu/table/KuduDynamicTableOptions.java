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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Kudu table options. */
@PublicEvolving
public class KuduDynamicTableOptions {

    public static final String IDENTIFIER = "kudu";

    public static final ConfigOption<String> KUDU_TABLE =
            ConfigOptions.key("kudu.table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's table name");

    public static final ConfigOption<String> KUDU_HASH_COLS =
            ConfigOptions.key("kudu.hash-columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's hash columns");

    public static final ConfigOption<Integer> KUDU_REPLICAS =
            ConfigOptions.key("kudu.replicas")
                    .intType()
                    .defaultValue(3)
                    .withDescription("kudu's replica nums");

    public static final ConfigOption<Integer> KUDU_MAX_BUFFER_SIZE =
            ConfigOptions.key("kudu.max-buffer-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("kudu's max buffer size");

    public static final ConfigOption<Duration> KUDU_FLUSH_INTERVAL =
            ConfigOptions.key("kudu.flush-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000))
                    .withDescription("kudu's data flush interval");

    public static final ConfigOption<Duration> KUDU_OPERATION_TIMEOUT =
            ConfigOptions.key("kudu.operation-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("kudu's operation timeout");

    public static final ConfigOption<Boolean> KUDU_IGNORE_NOT_FOUND =
            ConfigOptions.key("kudu.ignore-not-found")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if true, ignore all not found rows");

    public static final ConfigOption<Boolean> KUDU_IGNORE_DUPLICATE =
            ConfigOptions.key("kudu.ignore-duplicate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if true, ignore all duplicate rows");

    public static final ConfigOption<Integer> KUDU_HASH_PARTITION_NUMS =
            ConfigOptions.key("kudu.hash-partition-nums")
                    .intType()
                    .defaultValue(KUDU_REPLICAS.defaultValue() * 2)
                    .withDescription(
                            "kudu's hash partition bucket nums, defaultValue is 2 * replica nums");

    public static final ConfigOption<String> KUDU_PRIMARY_KEY_COLS =
            ConfigOptions.key("kudu.primary-key-columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's primary key, primary key must be ordered");

    public static final ConfigOption<Integer> KUDU_SCAN_ROW_SIZE =
            ConfigOptions.key("kudu.scan.row-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription("kudu's scan row size");

    private KuduDynamicTableOptions() {}
}
