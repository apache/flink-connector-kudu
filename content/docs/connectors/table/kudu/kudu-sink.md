---
title: DDL Operations using SQL
weight: 4
type: docs
aliases:
- /docs/connectors/table/kudu/kudu-sink.html
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

# Kudu Sink

The Kudu connector provides a `KuduSink` class that can be used to consume DataStreams and write the results into a Kudu table.

The constructor takes 3 or 4 arguments.

* `KuduWriterConfig` is used to specify the Kudu masters and the flush mode.
* `KuduTableInfo` identifies the table to be written
* `KuduOperationMapper` maps the records coming from the DataStream to a list of Kudu operations.
* `KuduFailureHandler` (optional): If you want to provide your own logic for handling writing failures.

The example below shows the creation of a sink for Row type records of 3 fields, and it Upserts each record.

It is assumed that a Kudu table with columns `col1, col2, col3` called `AlreadyExistingTable` exists. Note that if this were not the case,
we could pass a `KuduTableInfo` as described in the [Catalog - Creating a table](% link /docs/connectors/table/kudu/overview.html#creating-a-table %}) section of the Overview, and the sink would create the table with the provided configuration.

```java
KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(KUDU_MASTERS).build();

KuduSink<Row> sink = new KuduSink<>(
    writerConfig,
    KuduTableInfo.forTable("AlreadyExistingTable"),
    new RowOperationMapper<>(
            new String[]{"col1", "col2", "col3"},
            AbstractSingleOperationMapper.KuduOperation.UPSERT)
)
```
