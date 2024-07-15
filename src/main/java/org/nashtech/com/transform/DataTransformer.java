package org.nashtech.com.transform;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class DataTransformer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<String> schema;

    public DataTransformer(List<String> schema) {
        this.schema = schema;
    }

    public PCollection<Map<String, String>> parseAndValidate(PCollection<String> input) {
        return input.apply("Parse and Validate JSON", ParDo.of(new ParseAndValidateJson(schema)));
    }

    public PCollection<TableRow> convertToTableRows(PCollection<Map<String, String>> input) {
        return input.apply("Convert to TableRow", MapElements.via(new SerializableTableRowConverter(schema)));
    }

    private static class SerializableTableRowConverter extends SimpleFunction<Map<String, String>, TableRow> implements Serializable {

        private final List<String> schema;

        SerializableTableRowConverter(List<String> schema) {
            this.schema = schema;
        }

        @Override
        public TableRow apply(Map<String, String> row) {
            TableRow tableRow = new TableRow();
            schema.forEach(field -> tableRow.set(field, row.get(field)));
            return tableRow;
        }
    }
}