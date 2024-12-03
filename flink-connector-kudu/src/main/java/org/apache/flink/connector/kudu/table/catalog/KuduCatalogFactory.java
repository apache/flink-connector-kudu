/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kudu.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.kudu.shaded.com.google.common.collect.Sets;

import java.util.Set;

import static org.apache.flink.connector.kudu.table.KuduCommonOptions.KUDU_MASTERS;
import static org.apache.flink.connector.kudu.table.catalog.KuduCatalogOptions.DEFAULT_DATABASE;
import static org.apache.flink.connector.kudu.table.catalog.KuduCatalogOptions.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/** Factory for {@link KuduCatalog}. */
@Internal
public class KuduCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(KUDU_MASTERS);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(DEFAULT_DATABASE, PROPERTY_VERSION);
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        return new KuduCatalog(
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(KUDU_MASTERS));
    }
}
