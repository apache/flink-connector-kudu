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

import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.table.AbstractReadOnlyCatalog;
import org.apache.flink.connector.kudu.table.KuduDynamicTableFactory;
import org.apache.flink.connector.kudu.table.utils.KuduTableUtils;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.StringUtils;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableSet;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.apache.kudu.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kudu.table.KuduCommonOptions.KUDU_MASTERS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.KUDU_HASH_COLS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.KUDU_HASH_PARTITION_NUMS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.KUDU_PRIMARY_KEY_COLS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.KUDU_REPLICAS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.KUDU_TABLE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Catalog for reading and creating Kudu tables. */
public class KuduCatalog extends AbstractReadOnlyCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(KuduCatalog.class);
    private final KuduDynamicTableFactory tableFactory = new KuduDynamicTableFactory();
    private final String kuduMasters;
    private final KuduClient kuduClient;

    /**
     * Create a new {@link KuduCatalog} with the specified kudu master addresses.
     *
     * @param kuduMasters Connection address to Kudu
     */
    public KuduCatalog(String kuduMasters) {
        this("kudu", "default", kuduMasters);
    }

    /**
     * Create a new {@link KuduCatalog} with the specified catalog name, default database, and kudu
     * master addresses.
     *
     * @param catalogName Name of the catalog (used by the table environment)
     * @param defaultDatabase Default Kudu database name
     * @param kuduMasters Connection address to Kudu
     */
    public KuduCatalog(String catalogName, String defaultDatabase, String kuduMasters) {
        super(catalogName, defaultDatabase);
        this.kuduMasters = kuduMasters;
        this.kuduClient = createClient();
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(getKuduTableFactory());
    }

    public KuduDynamicTableFactory getKuduTableFactory() {
        return tableFactory;
    }

    private KuduClient createClient() {
        return new KuduClient.KuduClientBuilder(kuduMasters).build();
    }

    @Override
    public void open() {}

    @Override
    public void close() {
        try {
            if (kuduClient != null) {
                kuduClient.close();
            }
        } catch (KuduException e) {
            LOG.error("Error while closing kudu client", e);
        }
    }

    public ObjectPath getObjectPath(String tableName) {
        return new ObjectPath(getDefaultDatabase(), tableName);
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        try {
            return kuduClient.getTablesList().getTablesList();
        } catch (Throwable t) {
            throw new CatalogException("Could not list tables", t);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        checkNotNull(tablePath);
        try {
            return kuduClient.tableExists(tablePath.getObjectName());
        } catch (KuduException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public CatalogTable getTable(ObjectPath tablePath) throws TableNotExistException {
        checkNotNull(tablePath);

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String tableName = tablePath.getObjectName();

        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            return CatalogTable.of(
                    KuduTableUtils.kuduToFlinkSchema(kuduTable.getSchema()),
                    null,
                    Collections.emptyList(),
                    createTableProperties(tableName, kuduTable.getSchema().getPrimaryKeyColumns()));
        } catch (KuduException e) {
            throw new CatalogException(e);
        }
    }

    protected Map<String, String> createTableProperties(
            String tableName, List<ColumnSchema> primaryKeyColumns) {
        Map<String, String> props = new HashMap<>();
        props.put(KUDU_MASTERS.key(), kuduMasters);
        props.put(KUDU_TABLE.key(), tableName);
        String primaryKeyNames =
                primaryKeyColumns.stream()
                        .map(ColumnSchema::getName)
                        .collect(Collectors.joining(","));
        props.put(KUDU_PRIMARY_KEY_COLS.key(), primaryKeyNames);
        return props;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException {
        String tableName = tablePath.getObjectName();
        try {
            if (tableExists(tablePath)) {
                kuduClient.deleteTable(tableName);
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (KuduException e) {
            throw new CatalogException("Could not delete table " + tableName, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException {
        String tableName = tablePath.getObjectName();
        try {
            if (tableExists(tablePath)) {
                kuduClient.alterTable(tableName, new AlterTableOptions().renameTable(newTableName));
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (KuduException e) {
            throw new CatalogException("Could not rename table " + tableName, e);
        }
    }

    public void createTable(KuduTableInfo tableInfo, boolean ignoreIfExists)
            throws CatalogException, TableAlreadyExistException {
        checkNotNull(tableInfo);

        ObjectPath path = getObjectPath(tableInfo.getName());
        if (tableExists(path)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(getName(), path);
            }
        }

        try {
            kuduClient.createTable(
                    tableInfo.getName(), tableInfo.getSchema(), tableInfo.getCreateTableOptions());
        } catch (KuduException e) {
            throw new CatalogException("Could not create table " + tableInfo.getName(), e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException {
        checkNotNull(tablePath, "Table path must be provided.");
        checkNotNull(table, "Table must be provided.");
        checkArgument(table instanceof ResolvedCatalogBaseTable, "Table must be resolved.");

        Map<String, String> tableProperties = table.getOptions();
        ResolvedSchema schema = ((ResolvedCatalogBaseTable<?>) table).getResolvedSchema();

        Set<String> optionalProperties =
                ImmutableSet.of(
                        KUDU_REPLICAS.key(), KUDU_HASH_PARTITION_NUMS.key(), KUDU_HASH_COLS.key());

        Set<String> requiredProperties = new HashSet<>();
        if (!schema.getPrimaryKey().isPresent()) {
            requiredProperties.add(KUDU_PRIMARY_KEY_COLS.key());
        }

        if (!tableProperties.keySet().containsAll(requiredProperties)) {
            throw new CatalogException(
                    "Missing required property. The following properties must be provided: "
                            + requiredProperties);
        }

        Set<String> permittedProperties = Sets.union(requiredProperties, optionalProperties);
        if (!permittedProperties.containsAll(tableProperties.keySet())) {
            throw new CatalogException(
                    "Unpermitted properties were given. The following properties are allowed:"
                            + permittedProperties);
        }

        String tableName = tablePath.getObjectName();
        KuduTableInfo tableInfo =
                KuduTableUtils.createTableInfo(tableName, schema, tableProperties);

        createTable(tableInfo, ignoreIfExists);
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Lists.newArrayList(getDefaultDatabase());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (databaseName.equals(getDefaultDatabase())) {
            return new CatalogDatabaseImpl(new HashMap<>(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public List<String> listFunctions(String dbName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }
}
