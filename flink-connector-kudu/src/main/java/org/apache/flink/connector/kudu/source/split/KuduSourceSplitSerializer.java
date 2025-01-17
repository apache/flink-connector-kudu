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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** The class that serializes and deserializes {@link KuduSourceSplit}. */
public class KuduSourceSplitSerializer implements SimpleVersionedSerializer<KuduSourceSplit> {

    private static final int VERSION = 1; // Versioning for future changes

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(KuduSourceSplit obj) throws IOException {
        if (obj == null || obj.getSerializedScanToken() == null) {
            throw new IOException("KuduSourceSplit or serializedScanToken is null.");
        }

        return obj.getSerializedScanToken(); // Directly return the byte array
    }

    @Override
    public KuduSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        if (serialized == null || serialized.length == 0) {
            throw new IOException("Serialized data is empty or null.");
        }

        return new KuduSourceSplit(serialized); // Recreate the split with the byte array
    }
}
