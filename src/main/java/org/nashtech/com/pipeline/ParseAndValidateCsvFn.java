package org.nashtech.com.pipeline;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.Arrays;
import java.util.List;

public class ParseAndValidateCsvFn extends DoFn<String, List<String>> {
    private final List<String> schema;

    public ParseAndValidateCsvFn(List<String> schema) {
        this.schema = schema;
    }

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<List<String>> out) {
        String[] values = line.split(",");
        if (values.length != schema.size()) {
            throw new RuntimeException("Invalid CSV schema");
        }
        out.output(Arrays.asList(values));
    }
}
