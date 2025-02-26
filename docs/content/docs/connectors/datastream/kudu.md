---
title: Kudu
weight: 5
type: docs
aliases:
- /dev/connectors/kudu.html
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

# Apache Kudu Connector

Flink provides an [Apache Kudu](https://kudu.apache.org) connector for reading data from and writing data to Kudu tables.
Although it is possible to use the Kudu connector directly from the DataStream API, however we encourage all users to
explore the Table API as it provides a lot of useful tooling when working with Kudu data.

## Dependency

Apache Flink ships the connector for users to utilize.

To use the connector, add the following Maven dependency to your project:

{{< connector_artifact flink-connector-kudu kudu >}}

{{< hint info >}}
The current version of the connector is built with Kudu client version **1.17.1**.
{{< /hint >}}

## Installing Kudu

Follow the instructions from the [Kudu Installation Guide](https://kudu.apache.org/docs/installation.html).
Optionally, you can use the docker images provided in dockers folder.

## Kudu Source

TBA

## Kudu Sink

The connector provides a `KuduSink` class that can be used to consume data streams and write the results into a Kudu table.

### Usage

```java
DataStream<Row> ds = ...;

KuduSink<Row> sink = KuduSink.<Row>builder()
        .setWriterConfig(KuduWriterConfig.Builder.setMasters(KUDU_MASTERS).build())
        .setTableInfo(KuduTableInfo.forTable("already-existing-table"))
        .setOperationMapper(
                new RowOperationMapper<>(
                        new String[]{"col1", "col2", "col3"},
                        AbstractSingleOperationMapper.KuduOperation.UPSERT)
        )
        .build();

ds.sinkTo(sink);
```

The `KuduSink` builder takes 3 or 4 arguments:

* `KuduWriterConfig`: Used to specify the Kudu masters and the flush mode.
* `KuduTableInfo`: Identifies the table to be written.
* `KuduOperationMapper`: Maps the records coming from the `DataStream` to a list of Kudu operations. 
* `KuduFailureHandler` (optional): If you want to provide your own logic for handling writing failures.

### Kudu Operation Mapping

The connector supports `INSERT`, `UPSERT`, `UPDATE`, and `DELETE` operations.
The operation to be performed can vary dynamically based on the record.
To allow for more flexibility, it is also possible for one record to trigger 0, 1, or more operations.
For the highest level of control, implement the `KuduOperationMapper` interface.

If one record from the `DataStream` corresponds to one table operation, extend the `AbstractSingleOperationMapper` class.
An array of column names must be provided, which must match the schema of the Kudu table.

The `getField` method must be overridden, which extracts the value for the table column whose name is at the `i`th place
in the `columnNames` array. If the operation is one of (`CREATE, UPSERT, UPDATE, DELETE`) and does not depend on the
input record (constant during the life of the sink), it can be set in the constructor of `AbstractSingleOperationMapper`.
It is also possible to implement your own logic by overriding the `createBaseOperation` method that returns a
Kudu [Operation](https://kudu.apache.org/apidocs/org/apache/kudu/client/Operation.html).

There are pre-defined operation mappers for POJO, Flink `Row`, and Flink `Tuple` types for constant operation, 1-to-1 sinks:

* `PojoOperationMapper`: Each table column must correspond to a POJO field with the same name. The `columnNames` array 
  should contain those fields of the POJO that are present as table columns (the POJO fields can be a superset of table columns).
* `RowOperationMapper`/`TupleOperationMapper`: The mapping is based on position. The `i`th field of the `Row`/`Tuple`
  corresponds to the column of the table at the `i`th position in the `columnNames` array.

