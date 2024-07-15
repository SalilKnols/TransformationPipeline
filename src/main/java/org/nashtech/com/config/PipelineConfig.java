package org.nashtech.com.config;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PipelineConfig {
    private static final Logger logger = LoggerFactory.getLogger(PipelineConfig.class);
    private final Properties properties;

    public PipelineConfig(String propertiesFile) throws IOException {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(propertiesFile)) {
            if (input == null) {
                throw new IOException("Unable to find " + propertiesFile);
            }
            properties.load(input);
        }
        validateProperties();
    }

    private void validateProperties() {
        if (getBqTable() == null) {
            throw new IllegalArgumentException("Property 'bqTable' is required");
        }
        if (getBqProject() == null) {
            throw new IllegalArgumentException("Property 'bqProject' is required");
        }
        if (getBqDataset() == null) {
            throw new IllegalArgumentException("Property 'bqDataset' is required");
        }
        if (getSchema() == null || getSchema().isEmpty()) {
            throw new IllegalArgumentException("Property 'schema' is required and cannot be empty");
        }
        if (getTempLocation() == null) {
            throw new IllegalArgumentException("Property 'tempLocation' is required");
        }
    }

    public String getBqTable() {
        return properties.getProperty("bqTable");
    }

    public String getBqProject() {
        return properties.getProperty("bqProject");
    }

    public String getBqDataset() {
        return properties.getProperty("bqDataset");
    }

    public List<String> getSchema() {
        String schema = properties.getProperty("schema");
        return schema != null ? Arrays.asList(schema.split(",")) : null;
    }

    public List<String> getPersonalInfoColumns() {
        String personalInfoColumns = properties.getProperty("personalInfoColumns");
        return personalInfoColumns != null ? Arrays.asList(personalInfoColumns.split(",")) : null;
    }

    public String getKmsKeyUri() {
        return properties.getProperty("kmsKeyUri");
    }

    public String getTempLocation() {
        return properties.getProperty("tempLocation");
    }

    public String getPubsubSubscription() {
        return properties.getProperty("pubsubSubscription");
    }

    public PipelineOptions getPipelineOptions() {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setTempLocation(getTempLocation());
        options.setRunner(DirectRunner.class);
        if (options.getRunner().equals(DirectRunner.class)) {
            options.as(DirectOptions.class).setTargetParallelism(1);
        }
        return options;
    }
}
