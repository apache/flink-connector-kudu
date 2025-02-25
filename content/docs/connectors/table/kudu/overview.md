---
title: Overview
weight: 1
type: docs
aliases:
- /docs/connectors/table/kudu/
- /docs/connectors/table/kudu/overview.html
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

[Apache Kudu](https://kudu.apache.org) is a column-oriented, distributed data storage engine, specifically designed for use cases that require fast analytics on fast (rapidly changing) data. It lowers query latency significantly for Apache Flink and similar engines (Apache Impala, Apache NiFi, Apache Spark, and more).

This connector allows reading and writing to [Kudu](https://kudu.apache.org) by providing:

- a source (```KuduInputFormat```),
- a sink (```KuduSink```) and output (```KuduOutputFormat```)
- a table source (`KuduTableSource`),
- an upsert table sink (`KuduTableSink`),
- and a catalog (`KuduCatalog`).

It is also possible to use the Kudu connector directly from the DataStream API, however we
encourage all users to explore the [Table API](#sql-and-table-api) as it provides a lot of useful tooling when working
with Kudu data.

## Installing Kudu

Follow the instructions from the [Kudu Installation Guide](https://kudu.apache.org/docs/installation.html).
Optionally, you can use the docker images provided in dockers folder.

## Dependencies

To use this connector, add the following dependency to your project:

{{< connector_artifact flink-connector-kudu kudu >}}

*Version Compatibility*: The current version of the connector is built with Kudu client version **1.17.1**.

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/configuration/overview/).

## SQL and Table API

The Kudu connector is fully integrated with the Flink Table and SQL APIs. Once we configure the Kudu catalog (see the [next section](#configuring-the-kudu-catalog)), we can start querying or inserting into existing Kudu tables using the Flink SQL or Table API.

For more information about the possible queries, please check the [official documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/).

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

### DDL operations using SQL

It is possible to manipulate Kudu tables using SQL DDL. 

For more information, please refer to [DDL Operations using SQL]({% link /docs/connectors/table/kudu/ddl-operations.html %}).

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
* `CHAR` and `VARCHAR` are not yet supported - use `STRING`, which is a `VARCHAR(2147483647)`
* `DECIMAL` types are not yet supported

### Known limitations

* Data type limitations (see above).
* SQL Create table: primary keys can only be set by the `kudu.primary-key-columns` property, using the
  `PRIMARY KEY` constraint is not yet possible.
* SQL Create table: range partitioning is not supported.
* When getting a table through the Catalog, NOT NULL and PRIMARY KEY constraints are ignored. All columns
  are described as being nullable, and not being primary keys.
* Kudu tables cannot be altered through the catalog other than simple renaming
