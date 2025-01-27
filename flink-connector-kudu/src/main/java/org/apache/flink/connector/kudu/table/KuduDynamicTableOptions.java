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

import org.apache.kudu.client.SessionConfiguration;

import java.time.Duration;

/** Kudu table options. */
@PublicEvolving
public class KuduDynamicTableOptions {

    public static final String IDENTIFIER = "kudu";

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's table name");

    public static final ConfigOption<String> HASH_COLS =
            ConfigOptions.key("hash-columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's hash columns");

    public static final ConfigOption<String> PRIMARY_KEY_COLS =
            ConfigOptions.key("primary-key-columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's primary key, primary key must be ordered");

    public static final ConfigOption<Integer> REPLICAS =
            ConfigOptions.key("replicas")
                    .intType()
                    .defaultValue(3)
                    .withDescription("kudu's replica nums");

    public static final ConfigOption<Integer> HASH_PARTITION_NUMS =
            ConfigOptions.key("hash-partition-nums")
                    .intType()
                    .defaultValue(REPLICAS.defaultValue() * 2)
                    .withDescription(
                            "kudu's hash partition bucket nums, defaultValue is 2 * replicas");

    // -----------------------------------------------------------------------------------------
    // Sink options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<Integer> MAX_BUFFER_SIZE =
            ConfigOptions.key("sink.max-buffer-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("kudu's max buffer size");

    public static final ConfigOption<SessionConfiguration.FlushMode> FLUSH_MODE =
            ConfigOptions.key("sink.flush-mode")
                    .enumType(SessionConfiguration.FlushMode.class)
                    .defaultValue(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)
                    .withDescription("kudu's data flush mode");

    public static final ConfigOption<Duration> FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000))
                    .withDescription("kudu's data flush interval");

    public static final ConfigOption<Duration> OPERATION_TIMEOUT =
            ConfigOptions.key("sink.operation-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("kudu's operation timeout");

    public static final ConfigOption<Boolean> IGNORE_NOT_FOUND =
            ConfigOptions.key("sink.ignore-not-found")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if true, ignore all not found rows");

    public static final ConfigOption<Boolean> IGNORE_DUPLICATE =
            ConfigOptions.key("sink.ignore-duplicate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if true, ignore all duplicate rows");

    // -----------------------------------------------------------------------------------------
    // Scan options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<Integer> SCAN_ROW_SIZE =
            ConfigOptions.key("scan.row-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription("kudu's scan row size");

    private KuduDynamicTableOptions() {}
}
