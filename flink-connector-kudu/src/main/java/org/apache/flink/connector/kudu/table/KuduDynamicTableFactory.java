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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connector.kudu.table.utils.KuduTableUtils;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.kudu.shaded.com.google.common.collect.Sets;

import javax.annotation.Nullable;

import java.util.Set;

import static org.apache.flink.connector.kudu.table.KuduCommonOptions.MASTERS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.FLUSH_INTERVAL;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.FLUSH_MODE;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.HASH_COLS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.HASH_PARTITION_NUMS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.IDENTIFIER;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.IGNORE_DUPLICATE;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.IGNORE_NOT_FOUND;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.MAX_BUFFER_SIZE;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.OPERATION_TIMEOUT;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.PRIMARY_KEY_COLS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.REPLICAS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.SCAN_ROW_SIZE;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.TABLE_NAME;

/**
 * Factory for creating configured instances of {@link KuduDynamicTableSource}/{@link
 * KuduDynamicTableSink} in a stream environment.
 */
public class KuduDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(MASTERS);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(
                TABLE_NAME,
                HASH_COLS,
                HASH_PARTITION_NUMS,
                PRIMARY_KEY_COLS,
                SCAN_ROW_SIZE,
                REPLICAS,
                MAX_BUFFER_SIZE,
                OPERATION_TIMEOUT,
                FLUSH_MODE,
                FLUSH_INTERVAL,
                IGNORE_NOT_FOUND,
                IGNORE_DUPLICATE,
                LookupOptions.CACHE_TYPE,
                LookupOptions.PARTIAL_CACHE_MAX_ROWS,
                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
                LookupOptions.MAX_RETRIES);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final ReadableConfig config = getValidatedConfig(context);

        final String tableName =
                config.getOptional(TABLE_NAME)
                        .orElse(context.getObjectIdentifier().getObjectName());
        final ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        final KuduTableInfo tableInfo =
                KuduTableUtils.createTableInfo(
                        tableName, schema, context.getCatalogTable().toProperties());

        final KuduWriterConfig.Builder configBuilder =
                KuduWriterConfig.Builder.setMasters(config.get(MASTERS))
                        .setOperationTimeout(config.get(OPERATION_TIMEOUT).toMillis())
                        .setConsistency(config.get(FLUSH_MODE))
                        .setFlushInterval((int) config.get(FLUSH_INTERVAL).toMillis())
                        .setMaxBufferSize(config.get(MAX_BUFFER_SIZE))
                        .setIgnoreNotFound(config.get(IGNORE_NOT_FOUND))
                        .setIgnoreDuplicate(config.get(IGNORE_DUPLICATE));

        return new KuduDynamicTableSink(configBuilder, tableInfo, schema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final ReadableConfig config = getValidatedConfig(context);

        final String tableName =
                config.getOptional(TABLE_NAME)
                        .orElse(context.getObjectIdentifier().getObjectName());
        final KuduTableInfo tableInfo =
                KuduTableUtils.createTableInfo(
                        tableName,
                        context.getCatalogTable().getResolvedSchema(),
                        context.getCatalogTable().toProperties());

        final KuduReaderConfig.Builder readerConfigBuilder =
                KuduReaderConfig.Builder.setMasters(config.get(MASTERS))
                        .setRowLimit(config.get(SCAN_ROW_SIZE));

        return new KuduDynamicTableSource(
                readerConfigBuilder,
                tableInfo,
                context.getPhysicalRowDataType(),
                config.get(LookupOptions.MAX_RETRIES),
                getLookupCache(config));
    }

    private ReadableConfig getValidatedConfig(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        return helper.getOptions();
    }

    @Nullable
    private LookupCache getLookupCache(ReadableConfig config) {
        return LookupOptions.LookupCacheType.PARTIAL.equals(config.get(LookupOptions.CACHE_TYPE))
                ? DefaultLookupCache.fromConfig(config)
                : null;
    }
}
