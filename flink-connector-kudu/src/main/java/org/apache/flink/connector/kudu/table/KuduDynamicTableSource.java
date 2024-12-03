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

import org.apache.flink.connector.kudu.connector.KuduFilterInfo;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.converter.RowResultRowDataConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.format.KuduRowDataInputFormat;
import org.apache.flink.connector.kudu.table.function.lookup.KuduRowDataLookupFunction;
import org.apache.flink.connector.kudu.table.utils.KuduTableUtils;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import org.apache.commons.collections.CollectionUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A {@link DynamicTableSource} for Kudu. */
public class KuduDynamicTableSource
        implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                LookupTableSource,
                SupportsFilterPushDown {

    private final KuduTableInfo tableInfo;
    private final int lookupMaxRetryTimes;
    @Nullable private final LookupCache cache;
    private final transient List<KuduFilterInfo> predicates = new ArrayList<>();

    private KuduReaderConfig.Builder configBuilder;
    private DataType physicalRowDataType;
    private transient List<ResolvedExpression> filters;

    public KuduDynamicTableSource(
            KuduReaderConfig.Builder configBuilder,
            KuduTableInfo tableInfo,
            DataType physicalRowDataType,
            int lookupMaxRetryTimes,
            @Nullable LookupCache cache) {
        this.configBuilder = configBuilder;
        this.tableInfo = tableInfo;
        this.physicalRowDataType = physicalRowDataType;
        this.lookupMaxRetryTimes = lookupMaxRetryTimes;
        this.cache = cache;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            checkArgument(innerKeyArr.length == 1, "Kudu only supports non-nested lookup keys");
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
        }

        KuduRowDataLookupFunction lookupFunction =
                new KuduRowDataLookupFunction(
                        keyNames,
                        tableInfo,
                        configBuilder.build(),
                        DataType.getFieldNames(physicalRowDataType),
                        lookupMaxRetryTimes);

        return cache == null
                ? LookupFunctionProvider.of(lookupFunction)
                : PartialCachingLookupProvider.of(lookupFunction, cache);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        if (CollectionUtils.isNotEmpty(filters)) {
            for (ResolvedExpression filter : filters) {
                Optional<KuduFilterInfo> kuduFilterInfo = KuduTableUtils.toKuduFilterInfo(filter);
                if (kuduFilterInfo != null && kuduFilterInfo.isPresent()) {
                    predicates.add(kuduFilterInfo.get());
                }
            }
        }

        KuduRowDataInputFormat inputFormat =
                new KuduRowDataInputFormat(
                        configBuilder.build(),
                        new RowResultRowDataConverter(),
                        tableInfo,
                        predicates,
                        DataType.getFieldNames(physicalRowDataType));

        return InputFormatProvider.of(inputFormat);
    }

    @Override
    public DynamicTableSource copy() {
        return new KuduDynamicTableSource(
                configBuilder, tableInfo, physicalRowDataType, lookupMaxRetryTimes, cache);
    }

    @Override
    public String asSummaryString() {
        return "kudu";
    }

    @Override
    public boolean supportsNestedProjection() {
        //  planner doesn't support nested projection push down yet.
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KuduDynamicTableSource that = (KuduDynamicTableSource) o;
        return Objects.equals(configBuilder, that.configBuilder)
                && Objects.equals(tableInfo, that.tableInfo)
                && Objects.equals(cache, that.cache)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(filters, that.filters)
                && Objects.equals(predicates, that.predicates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                configBuilder, tableInfo, cache, physicalRowDataType, filters, predicates);
    }

    @Override
    public void applyLimit(long limit) {
        this.configBuilder = this.configBuilder.setRowLimit((int) limit);
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filters = filters;
        return Result.of(Collections.emptyList(), filters);
    }
}
