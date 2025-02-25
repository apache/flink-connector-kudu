---
title: Kudu
weight: 4
type: docs
aliases:
- /dev/table/connectors/kudu.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Kudu SQL Connector

This connector allows reading and writing to [Apache Kudu](https://kudu.apache.org) using SQL and the Table API.
This document describes how to setup the Kudu connector to run SQL queries with Kudu tables involved.

Dependencies
------------

{{< sql_connector_download_table "kudu" >}}

The Kudu connector is not part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

{{< hint info >}}
The current version of the connector is built with Kudu client version **1.17.1**.
{{< /hint >}}

How to create a Kudu table
--------------------------

A Kudu table can be defined as following:

```sql
CREATE TABLE MyKuduTable (
  id BIGINT,
  name STRING,
  age INT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kudu',
  'masters' = 'localhost:7051',
  'table-name' = 'user-table'
)
```

To also *create* a table in Kudu as well, the `hash-columns` options must be specified as a comma-delimited list.

Other properties, such as range partitioning cannot be configured here.
For more flexibility, please use `KuduCatalog#createTable` as described in [this]({{< ref "docs/dev/connectors/table/kudu" >}}#creating-a-kudu-table-directly-with-kuducatalog) section, or create the table directly in Kudu.

The `NOT NULL` constraint can be added to any of the column definitions.

Hash columns must be a subset of primary key columns.

SQL and Table API
-----------------

The Kudu connector is fully integrated with the Flink Table and SQL APIs.
Once we configure the Kudu catalog, we can start querying or inserting into existing Kudu tables using the Flink SQL or Table API.

For more information about the possible queries, please check the relevant parts of the documentation [here]({{< ref "/docs/dev/table/sql/overview/" >}}).

Kudu Catalog
------------

The connector comes with a catalog implementation to handle metadata about your Kudu setup and perform table management.
By using the Kudu catalog, you can access all the tables already created in Kudu from Flink SQL queries. The Kudu catalog only
allows users to manage Kudu tables. Tables using other data sources must be defined in other catalogs such as in-memory catalog or Hive catalog.

{{< tabs "71a3b36e-77b3-496a-9bbf-dcc4d1386ff8" >}}
{{< tab "SQL" >}}
```sql
CREATE CATALOG my_catalog WITH(
    'type' = 'kudu',
    'masters' = '...',
    'default-database' = '...'
);

USE CATALOG my_catalog;
```
{{< /tab >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tEnv = TableEnvironment.create(settings);

Configuration catalogConf = new Configuration();
catalogConf.set(KuduCommonOptions.MASTERS, "...");
catalogConf.set(KuduCatalogOptions.DEFAULT_DATABASE, "my_db");

CatalogDescriptor kuduCatalog = CatalogDescriptor.of("my_catalog", catalogConf);
tEnv.createCatalog("my_catalog", kuduCatalog);

// set the Kudu Catalog as the current catalog of the session
tEnv.useCatalog("my_catalog");
```
{{< /tab >}}
{{< tab "YAML" >}}
```yaml
execution:
    ...
    current-catalog: my_catalog  # set the target Kudu Catalog as the current catalog of the session
    current-database: my_db

catalogs:
   - name: my_catalog
     type: kudu
     masters: ...
     default-database: my_db
```
{{< /tab >}}
{{< /tabs >}}

### Creating a Kudu table directly with KuduCatalog

The KuduCatalog also exposes a simple `createTable` method that requires only the Kudu table configuration.
This includes schema, partitioning, replication, etc. that all can be specified via `KuduTableInfo`.

For this, use the `createTableIfNotExists` method, that:
- Takes a `ColumnSchemasFactory`, that implement `getColumnSchemas()`, returning a list of Kudu [ColumnSchema](https://kudu.apache.org/apidocs/org/apache/kudu/ColumnSchema.html) objects;
- Takes a `CreateTableOptionsFactory` parameter, that implement `getCreateTableOptions()`, returning a [CreateTableOptions](https://kudu.apache.org/apidocs/org/apache/kudu/client/CreateTableOptions.html) object.

The following example shows the creation of a table called `ExampleTable` with two columns.
Column `first` being a primary key, and configuration of replicas and hash partitioning.

```java
TableEnvironment tEnv = ...

// Assuming a Kudu catalog named "kudu" is registered.
KuduCatalog catalog = (KuduCatalog) tEnv.getCatalog("kudu").get();

KuduTableInfo tableInfo = KuduTableInfo
    .forTable("ExampleTable")
    .createTableIfNotExists(
        () ->
            Lists.newArrayList(
                new ColumnSchema
                    .ColumnSchemaBuilder("first", Type.INT32)
                    .key(true)
                    .build(),
                new ColumnSchema
                    .ColumnSchemaBuilder("second", Type.STRING)
                    .build()
            ),
        () -> new CreateTableOptions()
            .setNumReplicas(1)
            .addHashPartitions(Arrays.asList("first"), 2));

catalog.createTable(tableInfo, false);
```

The example uses lambda expressions to implement the functional interfaces.

You can read more about Kudu schema design in the [Kudu docs](https://kudu.apache.org/docs/schema_design.html).

Data Type Mapping
-----------------

| Flink/SQL      |      Kudu      |
| ---------------- | :---------------: |
| `STRING`       |     STRING     |
| `BOOLEAN`      |      BOOL      |
| `TINYINT`      |      INT8      |
| `SMALLINT`     |      INT16      |
| `INT`          |      INT32      |
| `BIGINT`       |      INT64      |
| `FLOAT`        |      FLOAT      |
| `DOUBLE`       |     DOUBLE     |
| `BYTES`        |     BINARY     |
| `TIMESTAMP(3)` | UNIXTIME_MICROS |

* `TIMESTAMP`s are fixed to a precision of 3, and the corresponding Java conversion class is `java.sql.Timestamp`
* `BINARY` and `VARBINARY` are not yet supported - use `BYTES`, which is a `VARBINARY(2147483647)`

## Known limitations

* Data type limitations (see above).
* SQL Create table: range partitioning is not supported.
* When getting a table through the Catalog, `NOT NULL` and `PRIMARY KEY` constraints are ignored. All columns are described as being nullable, and not being primary keys.
* Kudu tables cannot be altered through the catalog other than simple renaming

## Lookup Cache

The Kudu connector can be used in temporal join as a lookup source (a.k.a. dimension table).
The lookup cache is used to improve performance of temporal join the Kudu connector.
Currently, only sync lookup mode is supported.

By default, lookup cache is not enabled: all the requests are sent to external database.
You can enable it by setting `lookup.cache` to `PARTIAL`.

When enabled, each process (i.e. TaskManager) will hold a cache.
Flink will lookup the cache first, and only send requests to external database when cache missing, and update cache with the rows returned.
The oldest rows in cache will be expired when the cache hit to the max cached rows `lookup.partial-cache.max-rows` or when the row exceeds the max time to live specified by `lookup.partial-cache.expire-after-write` or `lookup.partial-cache.expire-after-access`.
The cached rows might not be the latest, users can tune expiration options to a smaller value to have a better fresh data, but this may increase the number of requests send to database.
So a balance should be kept between throughput and correctness.
