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

package org.apache.flink.connector.kudu.connector.writer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link KuduWriterConfig}. */
class KuduWriterConfigTest {

    @Test
    void testBuilderUsesMaxMutationBufferOps() {
        KuduWriterConfig config =
                KuduWriterConfig.Builder.setMasters("localhost")
                        .setMaxMutationBufferOps(42)
                        .build();

        assertEquals(42, config.getMaxMutationBufferOps());
        assertEquals(42, config.getMaxBufferSize());
    }

    @Test
    void testDeprecatedMaxBufferSizeSetterStillConfiguresMutationBufferOps() {
        KuduWriterConfig config =
                KuduWriterConfig.Builder.setMasters("localhost").setMaxBufferSize(24).build();

        assertEquals(24, config.getMaxMutationBufferOps());
        assertEquals(24, config.getMaxBufferSize());
    }
}
