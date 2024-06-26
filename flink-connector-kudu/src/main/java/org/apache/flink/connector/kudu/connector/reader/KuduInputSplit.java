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

package org.apache.flink.connector.kudu.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.LocatableInputSplit;

/** Kudu input split that holds a scan token to identify the split position. */
@Internal
public class KuduInputSplit extends LocatableInputSplit {

    private static final long serialVersionUID = 1L;

    private final byte[] scanToken;

    /**
     * Creates a new KuduInputSplit.
     *
     * @param splitNumber the number of the input split
     * @param hostnames the names of the hosts storing the data this input split refers to
     */
    public KuduInputSplit(byte[] scanToken, final int splitNumber, final String[] hostnames) {
        super(splitNumber, hostnames);

        this.scanToken = scanToken;
    }

    public byte[] getScanToken() {
        return scanToken;
    }
}
