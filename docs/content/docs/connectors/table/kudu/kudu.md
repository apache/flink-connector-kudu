---
title: Overview
weight: 1
type: docs
aliases:
- /docs/connectors/table/kudu/
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

# Apache Kudu

This connector allows reading and writing to [Apache Kudu](https://kudu.apache.org) using SQL and the Table API. Kudu is a column-oriented, distributed data storage engine, specifically designed for use cases that require fast analytics on fast (rapidly changing) data. It lowers query latency significantly for Apache Flink and similar engines (Apache Impala, Apache NiFi, Apache Spark, and more).

It is also possible to use the Kudu connector directly from the DataStream API, however we encourage all users to explore the Table API, as it provides a lot of useful tooling when working with Kudu data.

## Installing Kudu

Follow the instructions from the [Kudu Installation Guide](https://kudu.apache.org/docs/installation.html).
Optionally, you can use the docker images provided in dockers folder.

## Dependencies

To use this connector, add the following dependency to your project:

{{< sql_connector_download_table "kudu" >}}

*Version Compatibility*: The current version of the connector is built with Kudu client version **1.17.1**.

Note that the Kudu connector is not part of the binary distribution of Flink, and you need to link it into your job jar for cluster execution.
See how to link with them for cluster execution [here]({{< ref "/docs/dev/configuration/overview/" >}}).

## SQL and Table API

The Kudu connector is fully integrated with the Flink Table and SQL APIs. Once we configure the Kudu catalog (see the [next section](#configuring-the-kudu-catalog)), we can start querying or inserting into existing Kudu tables using the Flink SQL or Table API.

For more information about the possible queries, please check the [official documentation]({{< ref "/docs/dev/table/sql/overview/" >}}).

### Configuring the Kudu Catalog

The connector comes with a catalog implementation to handle metadata about your Kudu setup and perform table management.
By using the Kudu catalog, you can access all the tables already created in Kudu from Flink SQL queries. The Kudu catalog only
allows users to create or access existing Kudu tables. Tables using other data sources must be defined in other catalogs such as
in-memory catalog or Hive catalog.

When using the SQL CLI you can easily add the Kudu catalog to your environment yaml file:

```
catalogs:
  - name: kudu
    type: kudu
    kudu.masters: <host>:7051
```

Once the SQL CLI is started you can simply switch to the Kudu catalog by calling `USE CATALOG kudu;`.

You can also create and use the KuduCatalog directly in the Table environment:

```java
String KUDU_MASTERS="host1:port1,host2:port2"
KuduCatalog catalog = new KuduCatalog(KUDU_MASTERS);
tableEnv.registerCatalog("kudu", catalog);
tableEnv.useCatalog("kudu");
```

### Supported data types

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

Note:

* `TIMESTAMP`s are fixed to a precision of 3, and the corresponding Java conversion class is `java.sql.Timestamp`
* `BINARY` and `VARBINARY` are not yet supported - use `BYTES`, which is a `VARBINARY(2147483647)`

## Known limitations

* Data type limitations (see above).
* SQL Create table: primary keys can only be set by the `kudu.primary-key-columns` property, using the
  `PRIMARY KEY` constraint is not yet possible.
* SQL Create table: range partitioning is not supported.
* When getting a table through the Catalog, NOT NULL and PRIMARY KEY constraints are ignored. All columns
  are described as being nullable, and not being primary keys.
* Kudu tables cannot be altered through the catalog other than simple renaming

## DDL operations using SQL

To *create* a table, the additional properties `kudu.primary-key-columns` and `kudu.hash-columns` must be specified
as comma-delimited lists. Optionally, you can set the `kudu.replicas` property (defaults to 1).

Other properties, such as range partitioning, cannot be configured here - for more flexibility, please use
`catalog.createTable` as described in [this]({% link /docs/connectors/table/kudu/overview.html#Creating-a-KuduTable-directly-with-KuduCatalog %}) section in the Overview, or create the table directly in Kudu.

The `NOT NULL` constraint can be added to any of the column definitions. By setting a column as a primary key, it will automatically by created with the `NOT NULL` constraint.

Hash columns must be a subset of primary key columns.

### Kudu Catalog

```
CREATE TABLE TestTable (
  first STRING,
  second STRING,
  third INT NOT NULL
) WITH (
  'kudu.hash-columns' = 'first',
  'kudu.primary-key-columns' = 'first,second'
)
```

### Other catalogs

When not using the Kudu catalog, the following additional properties must be specified in the `WITH` clause:

* `'connector.type'='kudu'`
* `'kudu.masters'='host1:port1,host2:port2,...'`: comma-delimitered list of Kudu masters
* `'kudu.table'='...'`: The table's name within the Kudu database.

If you have registered and are using the Kudu catalog, these properties are handled automatically.

```
CREATE TABLE TestTable (
  first STRING,
  second STRING,
  third INT NOT NULL
) WITH (
  'connector.type' = 'kudu',
  'kudu.masters' = '...',
  'kudu.table' = 'TestTable',
  'kudu.hash-columns' = 'first',
  'kudu.primary-key-columns' = 'first,second'
)
```

### Renaming a table

Use the following to rename a table:

```sql
ALTER TABLE TestTable RENAME TO TestTableRen
```

### Dropping a table

Use the following to drop a table:

```sql
DROP TABLE TestTableRen
```

### Creating a Kafka table

The example below shows how to create a Kafka table:

```sql
CREATE TABLE KuduTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kudu',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
)
```

### Creating a KuduTable directly with KuduCatalog

The KuduCatalog also exposes a simple `createTable` method that required only the where table configuration,
including schema, partitioning, replication, etc. that can be specified using a `KuduTableInfo` object.

Use the `createTableIfNotExists` method, that:

- takes a `ColumnSchemasFactory`, that implement `getColumnSchemas()`, returning a list of Kudu [ColumnSchema](https://kudu.apache.org/apidocs/org/apache/kudu/ColumnSchema.html) objects;
- takes a a `CreateTableOptionsFactory` parameter, that implement `getCreateTableOptions()`, returning a [CreateTableOptions](https://kudu.apache.org/apidocs/org/apache/kudu/client/CreateTableOptions.html) object.

The follwoing example shows the creation of a table called `ExampleTable` with two columns, `first` being a primary key; and configuration of replicas and hash partitioning.

```java
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
            .addHashPartitions(Lists.newArrayList("first"), 2));

catalog.createTable(tableInfo, false);
```

The example uses lambda expressions to implement the functional interfaces.

You can read more about Kudu schema design in the [Kudu docs](https://kudu.apache.org/docs/schema_design.html).

## Lookup Cache

The Kudu connector can be used in temporal join as a lookup source (a.k.a. dimension table). The lookup cache is used to improve performance of temporal join the Kudu connector. Currently, only sync lookup mode is supported.

By default, lookup cache is not enabled: all the requests are sent to external database. You can enable it by setting both `lookup.cache.max-rows` and `lookup.cache.ttl`.

When enabled, each process (i.e. TaskManager) will hold a cache. Flink will lookup the cache first, and only send requests to external database when cache missing, and update cache with the rows returned. The oldest rows in cache will be expired when the cache hit to the max cached rows `kudu.lookup.cache.max-rows` or when the row exceeds the max time to live `kudu.lookup.cache.ttl`. The cached rows might not be the latest, users can tune `kudu.lookup.cache.ttl` to a smaller value to have a better fresh data, but this may increase the number of requests send to database. So this is a balance between throughput and correctness.

Reference: [Flink Jdbc Connector]({{< ref "/docs/connectors/table/jdbc/#lookup-cache" >}})
