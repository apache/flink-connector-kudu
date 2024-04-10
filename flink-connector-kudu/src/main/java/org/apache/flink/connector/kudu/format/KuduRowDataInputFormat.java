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

package org.apache.flink.connector.kudu.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kudu.connector.KuduFilterInfo;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** InputFormat based on the rowData object type. */
@PublicEvolving
public class KuduRowDataInputFormat extends AbstractKuduInputFormat<RowData> {

    public KuduRowDataInputFormat(
            KuduReaderConfig readerConfig,
            RowResultConverter<RowData> rowResultConverter,
            KuduTableInfo tableInfo) {
        super(readerConfig, rowResultConverter, tableInfo);
    }

    public KuduRowDataInputFormat(
            KuduReaderConfig readerConfig,
            RowResultConverter<RowData> rowResultConverter,
            KuduTableInfo tableInfo,
            List<String> tableProjections) {
        super(readerConfig, rowResultConverter, tableInfo, tableProjections);
    }

    public KuduRowDataInputFormat(
            KuduReaderConfig readerConfig,
            RowResultConverter<RowData> rowResultConverter,
            KuduTableInfo tableInfo,
            List<KuduFilterInfo> tableFilters,
            List<String> tableProjections) {
        super(readerConfig, rowResultConverter, tableInfo, tableFilters, tableProjections);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }
}
