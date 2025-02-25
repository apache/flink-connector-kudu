---
title: DDL Operations using SQL
weight: 2
type: docs
aliases:
- /docs/connectors/table/kudu/ddl-operations.html
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

# DDL operations using SQL

To *create* a table, the additional properties `kudu.primary-key-columns` and `kudu.hash-columns` must be specified
as comma-delimited lists. Optionally, you can set the `kudu.replicas` property (defaults to 1).

Other properties, such as range partitioning, cannot be configured here - for more flexibility, please use
`catalog.createTable` as described in [this]({% link /docs/connectors/table/kudu/overview.html#Creating-a-KuduTable-directly-with-KuduCatalog %}) section in the Overview, or create the table directly in Kudu.

The `NOT NULL` constraint can be added to any of the column definitions. By setting a column as a primary key, it will automatically by created with the `NOT NULL` constraint.

Hash columns must be a subset of primary key columns.

#### Kudu Catalog

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

#### Other catalogs

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

#### Renaming a table

Use the following to rename a table:

```sql
ALTER TABLE TestTable RENAME TO TestTableRen
```

#### Dropping a table

Use the following to drop a table:

```sql
DROP TABLE TestTableRen
```

#### Creating a Kafka table

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

#### Creating a KuduTable directly with KuduCatalog

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
