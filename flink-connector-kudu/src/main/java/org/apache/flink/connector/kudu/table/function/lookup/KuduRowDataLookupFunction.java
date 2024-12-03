/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kudu.table.function.lookup;

import org.apache.flink.connector.kudu.connector.KuduFilterInfo;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;
import org.apache.flink.connector.kudu.connector.converter.RowResultRowDataConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connector.kudu.connector.reader.KuduReader;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** LookupFunction based on the RowData object type. */
public class KuduRowDataLookupFunction extends LookupFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KuduRowDataLookupFunction.class);

    private final KuduTableInfo tableInfo;
    private final KuduReaderConfig kuduReaderConfig;
    private final String[] keyNames;
    private final List<String> projectedFields;
    private final int maxRetryTimes;
    private final RowResultConverter<RowData> convertor;

    private transient KuduReader<RowData> kuduReader;

    public KuduRowDataLookupFunction(
            String[] keyNames,
            KuduTableInfo tableInfo,
            KuduReaderConfig kuduReaderConfig,
            List<String> projectedFields) {
        this(keyNames, tableInfo, kuduReaderConfig, projectedFields, 1);
    }

    public KuduRowDataLookupFunction(
            String[] keyNames,
            KuduTableInfo tableInfo,
            KuduReaderConfig kuduReaderConfig,
            List<String> projectedFields,
            int maxRetryTimes) {
        this.tableInfo = tableInfo;
        this.projectedFields = projectedFields;
        this.keyNames = keyNames;
        this.kuduReaderConfig = kuduReaderConfig;
        this.maxRetryTimes = maxRetryTimes;
        convertor = new RowResultRowDataConverter();
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            try {
                List<KuduFilterInfo> kuduFilterInfos = buildKuduFilterInfo((GenericRowData) keyRow);
                this.kuduReader.setTableFilters(kuduFilterInfos);
                KuduInputSplit[] inputSplits = kuduReader.createInputSplits(1);
                ArrayList<RowData> rows = new ArrayList<>();
                for (KuduInputSplit inputSplit : inputSplits) {
                    KuduReaderIterator<RowData> scanner =
                            kuduReader.scanner(inputSplit.getScanToken());
                    while (scanner.hasNext()) {
                        RowData row = scanner.next();
                        rows.add(row);
                    }
                }
                rows.trimToSize();
                return rows;
            } catch (Exception e) {
                LOG.error(String.format("Kudu scan error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of Kudu scan failed.", e);
                }
                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void open(FunctionContext context) {
        try {
            super.open(context);
            this.kuduReader =
                    new KuduReader<>(this.tableInfo, this.kuduReaderConfig, this.convertor);
            // build kudu cache
            this.kuduReader.setTableProjections(
                    CollectionUtil.isNullOrEmpty(projectedFields) ? null : projectedFields);
        } catch (Exception ioe) {
            LOG.error("Exception while creating connection to Kudu.", ioe);
            throw new RuntimeException("Cannot create connection to Kudu.", ioe);
        }
    }

    @Override
    public void close() {
        if (kuduReader != null) {
            try {
                kuduReader.close();
                kuduReader = null;
            } catch (IOException e) {
                // ignore exception when close.
                LOG.warn("Failed to close Kudu table reader", e);
            }
        }
    }

    private List<KuduFilterInfo> buildKuduFilterInfo(GenericRowData keyRow) {
        return IntStream.range(0, keyNames.length)
                .mapToObj(
                        i ->
                                KuduFilterInfo.Builder.create(keyNames[i])
                                        .equalTo(keyRow.getField(i))
                                        .build())
                .collect(Collectors.toList());
    }
}
