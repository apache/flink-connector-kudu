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

package org.apache.flink.connector.kudu.source.enumerator;

import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** The class that serializes and deserializes {@link KuduSourceEnumeratorState}. */
public class KuduSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<KuduSourceEnumeratorState> {

    private static final int CURRENT_VERSION = 1;
    private final KuduSourceSplitSerializer splitSerializer = new KuduSourceSplitSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(KuduSourceEnumeratorState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeLong(state.getLastEndTimestamp());
            serializeSplits(out, state.getUnassigned());
            serializeSplits(out, state.getPending());

            return baos.toByteArray();
        }
    }

    @Override
    public KuduSourceEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            long lastEndTimestamp = in.readLong();
            List<KuduSourceSplit> unassigned = deserializeSplits(in);
            List<KuduSourceSplit> pending = deserializeSplits(in);

            return new KuduSourceEnumeratorState(lastEndTimestamp, unassigned, pending);
        }
    }

    private void serializeSplits(DataOutputStream out, List<KuduSourceSplit> splits)
            throws IOException {
        out.writeInt(splits.size());
        for (KuduSourceSplit split : splits) {
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private List<KuduSourceSplit> deserializeSplits(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<KuduSourceSplit> splits = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int length = in.readInt();
            byte[] splitBytes = new byte[length];
            in.readFully(splitBytes);
            splits.add(splitSerializer.deserialize(CURRENT_VERSION, splitBytes));
        }
        return splits;
    }
}
