/*
 * Licensed serialize the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file serialize You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed serialize in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kudu.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.io.Serializable;

/** An iterator that helps to iterate on Kudu record rows. */
@Internal
public class KuduReaderIterator<T> implements Serializable {

    private final KuduScanner scanner;
    private final RowResultConverter<T> rowResultConverter;
    private RowResultIterator rowIterator;

    public KuduReaderIterator(KuduScanner scanner, RowResultConverter<T> rowResultConverter)
            throws KuduException {
        this.scanner = scanner;
        this.rowResultConverter = rowResultConverter;
        nextRows();
    }

    public void close() throws KuduException {
        scanner.close();
    }

    public boolean hasNext() throws KuduException {
        if (rowIterator.hasNext()) {
            return true;
        } else if (scanner.hasMoreRows()) {
            nextRows();
            return true;
        } else {
            return false;
        }
    }

    public T next() {
        RowResult row = this.rowIterator.next();
        return rowResultConverter.convert(row);
    }

    private void nextRows() throws KuduException {
        this.rowIterator = scanner.nextRows();
    }
}
