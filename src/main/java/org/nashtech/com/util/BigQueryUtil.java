package org.nashtech.com.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.List;

public class BigQueryUtil {

    /**
     * Creates a BigQuery TableSchema from a list of schema field names.
     * Each field is assigned the STRING type.
     *
     * @param schemaFields List of field names for the schema
     * @return TableSchema object with the specified fields
     */
    public static TableSchema createBigQuerySchema(List<String> schemaFields) {
        if (schemaFields == null || schemaFields.isEmpty()) {
            throw new IllegalArgumentException("Schema fields list cannot be null or empty");
        }

        List<TableFieldSchema> fields = new ArrayList<>();
        for (String field : schemaFields) {
            if (field == null || field.isEmpty()) {
                throw new IllegalArgumentException("Field names cannot be null or empty");
            }
            fields.add(new TableFieldSchema().setName(field).setType("STRING"));
        }
        return new TableSchema().setFields(fields);
    }
}