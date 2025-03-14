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

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.List;

/**
 * A source event use to signal from {@link
 * org.apache.flink.connector.kudu.source.reader.KuduSourceReader} to {@link
 * org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumerator} the finished splits. This
 * allows us to differentiate between enumerated but unassigned and pending splits in the
 * enumerator.
 */
public class SplitFinishedEvent implements SourceEvent {

    private final List<KuduSourceSplit> finishedSplits;

    public SplitFinishedEvent(List<KuduSourceSplit> finishedSplits) {
        this.finishedSplits = finishedSplits;
    }

    public List<KuduSourceSplit> getFinishedSplits() {
        return finishedSplits;
    }
}
