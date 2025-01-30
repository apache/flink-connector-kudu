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

package org.apache.flink.connector.kudu.table.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kudu.connector.ColumnSchemasFactory;
import org.apache.flink.connector.kudu.connector.CreateTableOptionsFactory;
import org.apache.flink.connector.kudu.connector.KuduFilterInfo;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.HASH_COLS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.HASH_PARTITIONS;
import static org.apache.flink.connector.kudu.table.KuduDynamicTableOptions.REPLICAS;

/** Kudu table utilities. */
public class KuduTableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KuduTableUtils.class);

    public static KuduTableInfo createTableInfo(
            String tableName, ResolvedSchema schema, Map<String, String> props) {
        KuduTableInfo tableInfo = KuduTableInfo.forTable(tableName);

        if (!schema.getPrimaryKey().isPresent()) {
            LOG.info(
                    "Primary key is not defined in the Flink table schema, assuming the table in Kudu already exists.");
            return tableInfo;
        }

        List<Tuple2<String, DataType>> columns =
                schema.getColumns().stream()
                        .map(col -> Tuple2.of(col.getName(), col.getDataType()))
                        .collect(Collectors.toList());

        List<String> primaryKeyCols = schema.getPrimaryKey().get().getColumns();
        ColumnSchemasFactory schemasFactory = () -> toKuduConnectorColumns(columns, primaryKeyCols);
        int replicas =
                Optional.ofNullable(props.get(REPLICAS.key()))
                        .map(Integer::parseInt)
                        .orElse(REPLICAS.defaultValue());
        List<String> hashCols = getHashColumns(props).orElse(primaryKeyCols);
        int hashPartitions =
                Optional.ofNullable(props.get(HASH_PARTITIONS.key()))
                        .map(Integer::parseInt)
                        .orElse(HASH_PARTITIONS.defaultValue());
        CreateTableOptionsFactory optionsFactory =
                () ->
                        new CreateTableOptions()
                                .setNumReplicas(replicas)
                                .addHashPartitions(hashCols, hashPartitions);

        tableInfo.createTableIfNotExists(schemasFactory, optionsFactory);

        return tableInfo;
    }

    public static List<ColumnSchema> toKuduConnectorColumns(
            List<Tuple2<String, DataType>> columns, Collection<String> keyColumns) {
        return columns.stream()
                .map(
                        t -> {
                            ColumnSchema.ColumnSchemaBuilder builder =
                                    new ColumnSchema.ColumnSchemaBuilder(
                                                    t.f0, KuduTypeUtils.toKuduType(t.f1))
                                            .key(keyColumns.contains(t.f0))
                                            .nullable(
                                                    !keyColumns.contains(t.f0)
                                                            && t.f1.getLogicalType().isNullable());
                            if (t.f1.getLogicalType() instanceof DecimalType) {
                                DecimalType decimalType = ((DecimalType) t.f1.getLogicalType());
                                builder.typeAttributes(
                                        new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                                                .precision(decimalType.getPrecision())
                                                .scale(decimalType.getScale())
                                                .build());
                            }
                            return builder.build();
                        })
                .collect(Collectors.toList());
    }

    public static org.apache.flink.table.api.Schema kuduToFlinkSchema(Schema schema) {
        org.apache.flink.table.api.Schema.Builder builder =
                org.apache.flink.table.api.Schema.newBuilder();

        for (ColumnSchema column : schema.getColumns()) {
            DataType flinkType =
                    KuduTypeUtils.toFlinkType(column.getType(), column.getTypeAttributes())
                            .nullable();
            builder.column(column.getName(), flinkType);
        }

        return builder.build();
    }

    public static Optional<List<String>> getHashColumns(Map<String, String> tableProps) {
        if (!tableProps.containsKey(HASH_COLS.key())) {
            return Optional.empty();
        }
        return Optional.of(Arrays.asList(tableProps.get(HASH_COLS.key()).split(",")));
    }

    /** Converts Flink Expression to KuduFilterInfo. */
    @Nullable
    public static Optional<KuduFilterInfo> toKuduFilterInfo(Expression predicate) {
        LOG.debug(
                "predicate summary: [{}], class: [{}], children: [{}]",
                predicate.asSummaryString(),
                predicate.getClass(),
                predicate.getChildren());
        if (predicate instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) predicate;
            FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
            List<Expression> children = callExpression.getChildren();
            if (children.size() == 1) {
                return convertUnaryIsNullExpression(functionDefinition, children);
            } else if (children.size() == 2
                    && !functionDefinition.equals(BuiltInFunctionDefinitions.OR)) {
                return convertBinaryComparison(functionDefinition, children);
            } else if (!children.isEmpty()
                    && functionDefinition.equals(BuiltInFunctionDefinitions.OR)) {
                return convertIsInExpression(children);
            }
        }
        return Optional.empty();
    }

    private static boolean isFieldReferenceExpression(Expression exp) {
        return exp instanceof FieldReferenceExpression;
    }

    private static boolean isValueLiteralExpression(Expression exp) {
        return exp instanceof ValueLiteralExpression;
    }

    private static Optional<KuduFilterInfo> convertUnaryIsNullExpression(
            FunctionDefinition functionDefinition, List<Expression> children) {
        FieldReferenceExpression fieldReferenceExpression;
        if (isFieldReferenceExpression(children.get(0))) {
            fieldReferenceExpression = (FieldReferenceExpression) children.get(0);
        } else {
            return Optional.empty();
        }
        // IS_NULL IS_NOT_NULL
        String columnName = fieldReferenceExpression.getName();
        KuduFilterInfo.Builder builder = KuduFilterInfo.Builder.create(columnName);
        if (functionDefinition.equals(BuiltInFunctionDefinitions.IS_NULL)) {
            return Optional.of(builder.isNull().build());
        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.IS_NOT_NULL)) {
            return Optional.of(builder.isNotNull().build());
        }
        return Optional.empty();
    }

    private static Optional<KuduFilterInfo> convertBinaryComparison(
            FunctionDefinition functionDefinition, List<Expression> children) {
        FieldReferenceExpression fieldReferenceExpression;
        ValueLiteralExpression valueLiteralExpression;
        if (isFieldReferenceExpression(children.get(0))
                && isValueLiteralExpression(children.get(1))) {
            fieldReferenceExpression = (FieldReferenceExpression) children.get(0);
            valueLiteralExpression = (ValueLiteralExpression) children.get(1);
        } else if (isValueLiteralExpression(children.get(0))
                && isFieldReferenceExpression(children.get(1))) {
            fieldReferenceExpression = (FieldReferenceExpression) children.get(1);
            valueLiteralExpression = (ValueLiteralExpression) children.get(0);
        } else {
            return Optional.empty();
        }
        String columnName = fieldReferenceExpression.getName();
        Object value = extractValueLiteral(fieldReferenceExpression, valueLiteralExpression);
        if (value == null) {
            return Optional.empty();
        }
        KuduFilterInfo.Builder builder = KuduFilterInfo.Builder.create(columnName);
        // GREATER GREATER_EQUAL EQUAL LESS LESS_EQUAL
        if (functionDefinition.equals(BuiltInFunctionDefinitions.GREATER_THAN)) {
            return Optional.of(builder.greaterThan(value).build());
        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL)) {
            return Optional.of(builder.greaterOrEqualTo(value).build());
        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS)) {
            return Optional.of(builder.equalTo(value).build());
        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.LESS_THAN)) {
            return Optional.of(builder.lessThan(value).build());
        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL)) {
            return Optional.of(builder.lessOrEqualTo(value).build());
        }
        return Optional.empty();
    }

    private static Optional<KuduFilterInfo> convertIsInExpression(List<Expression> children) {
        // IN operation will be: or(equals(field, value1), equals(field, value2), ...) in blink
        // For FilterType IS_IN, all internal CallExpression's function need to be equals and
        // fields need to be same
        List<Object> values = new ArrayList<>(children.size());
        String columnName = "";
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) children.get(i);
                FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
                List<Expression> subChildren = callExpression.getChildren();
                FieldReferenceExpression fieldReferenceExpression;
                ValueLiteralExpression valueLiteralExpression;
                if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS)
                        && subChildren.size() == 2
                        && isFieldReferenceExpression(subChildren.get(0))
                        && isValueLiteralExpression(subChildren.get(1))) {
                    fieldReferenceExpression = (FieldReferenceExpression) subChildren.get(0);
                    valueLiteralExpression = (ValueLiteralExpression) subChildren.get(1);
                    String fieldName = fieldReferenceExpression.getName();
                    if (i != 0 && !columnName.equals(fieldName)) {
                        return Optional.empty();
                    } else {
                        columnName = fieldName;
                    }
                    Object value =
                            extractValueLiteral(fieldReferenceExpression, valueLiteralExpression);
                    if (value == null) {
                        return Optional.empty();
                    }
                    values.add(i, value);
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.empty();
            }
        }
        KuduFilterInfo.Builder builder = KuduFilterInfo.Builder.create(columnName);
        return Optional.of(builder.isIn(values).build());
    }

    private static Object extractValueLiteral(
            FieldReferenceExpression fieldReferenceExpression,
            ValueLiteralExpression valueLiteralExpression) {
        DataType fieldType = fieldReferenceExpression.getOutputDataType();
        return valueLiteralExpression.getValueAs(fieldType.getConversionClass()).orElse(null);
    }
}
