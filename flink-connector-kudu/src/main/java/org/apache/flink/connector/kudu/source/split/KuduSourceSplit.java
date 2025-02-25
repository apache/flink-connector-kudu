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

package org.apache.flink.connector.kudu.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Base64;

/** The Kudu source split that wraps a Kudu scan token. */
public class KuduSourceSplit implements SourceSplit, Serializable {

    // Serialized byte[] is already compact and Flink-optimized, whereas the
    // full KuduScanToken object might include unnecessary metadata or complex internal structures.
    private final byte[] serializedScanToken;

    public KuduSourceSplit(byte[] serializedScanToken) {
        this.serializedScanToken = serializedScanToken;
    }

    public byte[] getSerializedScanToken() {
        return serializedScanToken;
    }

    @Override
    public String splitId() {
        return Base64.getEncoder().encodeToString(serializedScanToken);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        KuduSourceSplit that = (KuduSourceSplit) obj;
        return Arrays.equals(serializedScanToken, that.serializedScanToken);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(serializedScanToken);
    }

    @Override
    public String toString() {
        return "KuduSourceSplit{" + "splitId='" + splitId() + "'" + '}';
    }
}
