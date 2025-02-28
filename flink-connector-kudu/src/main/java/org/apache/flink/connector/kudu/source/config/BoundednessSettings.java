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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Settings that define whether the data source operates in bounded or continuous unbounded mode
 * and, if unbounded, specify the discovery interval.
 *
 * <p>Key properties:
 *
 * <ul>
 *   <li>{@link Boundedness} - Determines whether the source operates in bounded or unbounded mode.
 *   <li>{@code discoveryInterval} - Specifies the interval for continuous discovery in unbounded
 *       mode. This is {@code null} when operating in bounded mode.
 * </ul>
 *
 * <p>When {@code boundedness} is set to {@link Boundedness#BOUNDED}, the source performs a single
 * snapshot read. When set to {@link Boundedness#CONTINUOUS_UNBOUNDED}, the source continuously
 * discovers new records based on the configured {@code discoveryInterval}.
 */
@PublicEvolving
public class BoundednessSettings implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Boundedness boundedness;
    private @Nullable final Duration discoveryInterval;

    public BoundednessSettings(Boundedness boundedness, @Nullable Duration discoveryInterval) {
        this.boundedness = checkNotNull(boundedness);
        this.discoveryInterval = discoveryInterval;
    }

    public Duration getDiscoveryInterval() {
        return discoveryInterval;
    }

    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public String toString() {
        return "ContinuousUnBoundingSettings{"
                + "discoveryInterval='"
                + discoveryInterval
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

        BoundednessSettings that = (BoundednessSettings) object;
        return Objects.equals(discoveryInterval, that.discoveryInterval)
                && Objects.equals(boundedness, that.boundedness);
    }

    @Override
    public int hashCode() {
        return Objects.hash(discoveryInterval, boundedness);
    }
}
