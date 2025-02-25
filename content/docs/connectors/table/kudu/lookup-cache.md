---
title: Lookup Cache
weight: 3
type: docs
aliases:
- /docs/connectors/table/kudu/lookup-cache.html
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

# Lookup Cache

The Kudu connector can be used in temporal join as a lookup source (a.k.a. dimension table). The lookup cache is used to improve performance of temporal join the Kudu connector. Currently, only sync lookup mode is supported.

By default, lookup cache is not enabled: all the requests are sent to external database. You can enable it by setting both `lookup.cache.max-rows` and `lookup.cache.ttl`.

When enabled, each process (i.e. TaskManager) will hold a cache. Flink will lookup the cache first, and only send requests to external database when cache missing, and update cache with the rows returned. The oldest rows in cache will be expired when the cache hit to the max cached rows `kudu.lookup.cache.max-rows` or when the row exceeds the max time to live `kudu.lookup.cache.ttl`. The cached rows might not be the latest, users can tune `kudu.lookup.cache.ttl` to a smaller value to have a better fresh data, but this may increase the number of requests send to database. So this is a balance between throughput and correctness.

Reference: [Flink Jdbc Connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/jdbc/#lookup-cache)
