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

package org.apache.flink.connector.kudu.source.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Settings describing how to do continuous data discovery and enumeration for the data source's
 * continuous discovery and streaming mode.
 */
@PublicEvolving
public class ContinuousBoundingSettings implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Boundedness boundedness;
    private final Duration period;

    public ContinuousBoundingSettings(Boundedness boundedness, Duration discoveryInterval) {
        this.boundedness = checkNotNull(boundedness);
        this.period = checkNotNull(discoveryInterval);
    }

    public Duration getPeriod() {
        return period;
    }

    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public String toString() {
        return "ContinuousUnBoundingSettings{"
                + "period='"
                + period
                + "', boundedness='"
                + boundedness
                + "'}";
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        ContinuousBoundingSettings that = (ContinuousBoundingSettings) object;
        return Objects.equals(period, that.period) && Objects.equals(boundedness, that.boundedness);
    }

    @Override
    public int hashCode() {
        return Objects.hash(period, boundedness);
    }
}
