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

import java.io.Serializable;
import java.util.List;

/** The class storing state information for {@link KuduSourceEnumerator}. */
public class KuduSourceEnumeratorState implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long lastEndTimestamp;
    private final List<KuduSourceSplit> unassigned;
    private final List<KuduSourceSplit> pending;

    public KuduSourceEnumeratorState(
            long lastEndTimestamp,
            List<KuduSourceSplit> unassigned,
            List<KuduSourceSplit> pending) {
        this.lastEndTimestamp = lastEndTimestamp;
        this.unassigned = unassigned;
        this.pending = pending;
    }

    long getLastEndTimestamp() {
        return lastEndTimestamp;
    }

    List<KuduSourceSplit> getUnassigned() {
        return unassigned;
    }

    List<KuduSourceSplit> getPending() {
        return pending;
    }
}
