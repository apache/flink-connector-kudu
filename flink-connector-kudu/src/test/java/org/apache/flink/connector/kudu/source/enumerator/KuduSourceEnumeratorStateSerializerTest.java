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

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KuduSourceEnumeratorStateSerializer}. */
public class KuduSourceEnumeratorStateSerializerTest {
    private final KuduSourceEnumeratorStateSerializer serializer =
            new KuduSourceEnumeratorStateSerializer();

    @Test
    public void testSerializeDeserialize() throws IOException {
        byte[] token1 = {1, 2, 3};
        byte[] token2 = {4, 5, 6};
        byte[] token3 = {7, 8, 9};
        byte[] token4 = {10, 11, 12};

        List<KuduSourceSplit> unassigned =
                Arrays.asList(new KuduSourceSplit(token1), new KuduSourceSplit(token2));
        List<KuduSourceSplit> pending =
                Arrays.asList(new KuduSourceSplit(token3), new KuduSourceSplit(token4));
        KuduSourceEnumeratorState state =
                new KuduSourceEnumeratorState(12345L, unassigned, pending);

        byte[] serialized = serializer.serialize(state);
        KuduSourceEnumeratorState deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.getLastEndTimestamp()).isEqualTo(state.getLastEndTimestamp());
        assertThat(deserialized.getUnassigned()).hasSameSizeAs(state.getUnassigned());
        assertThat(deserialized.getPending()).hasSameSizeAs(state.getPending());

        for (int i = 0; i < unassigned.size(); i++) {
            byte[] expected = unassigned.get(i).getSerializedScanToken();
            assertThat(deserialized.getUnassigned().get(i).getSerializedScanToken())
                    .isEqualTo(expected);
        }

        for (int i = 0; i < pending.size(); i++) {
            byte[] expected = pending.get(i).getSerializedScanToken();
            assertThat(deserialized.getPending().get(i).getSerializedScanToken())
                    .isEqualTo(expected);
        }
    }

    @Test
    public void testSerializeDeserializeEmptyLists() throws IOException {
        KuduSourceEnumeratorState state =
                new KuduSourceEnumeratorState(
                        67890L, Collections.emptyList(), Collections.emptyList());

        byte[] serialized = serializer.serialize(state);
        KuduSourceEnumeratorState deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(state.getLastEndTimestamp()).isEqualTo(deserialized.getLastEndTimestamp());
        assertThat(deserialized.getUnassigned()).isEmpty();
        assertThat(deserialized.getPending()).isEmpty();
    }
}
