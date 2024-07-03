package org.nashtech.com.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.List;
import java.util.stream.Collectors;

public class BigQueryUtil {
    public static TableSchema createBigQuerySchema(List<String> schema) {
        List<TableFieldSchema> fields = schema.stream()
                .map(fieldName -> new TableFieldSchema().setName(fieldName).setType("STRING"))
                .collect(Collectors.toList());
        return new TableSchema().setFields(fields);
    }
}
