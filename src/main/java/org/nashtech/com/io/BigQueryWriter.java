package org.nashtech.com.io;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.nashtech.com.config.PipelineConfig;
import org.nashtech.com.util.BigQueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryWriter {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryWriter.class);
    private final PipelineConfig config;

    public BigQueryWriter(PipelineConfig config) {
        this.config = config;
    }

    public void writeToTable(PCollection<TableRow> tableRows) {

        tableRows.apply("Write to BigQuery", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.%s", config.getBqProject(), config.getBqDataset(), config.getBqTable()))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withSchema(BigQueryUtil.createBigQuerySchema(config.getSchema()))
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(config.getTempLocation())));
    }
}
