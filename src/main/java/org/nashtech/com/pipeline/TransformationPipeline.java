package org.nashtech.com.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;
import org.nashtech.com.config.PipelineConfig;
import org.nashtech.com.io.BigQueryWriter;
import org.nashtech.com.io.FileProcessor;
import org.nashtech.com.io.PubSubReader;
import org.nashtech.com.service.EncryptionService;
import org.nashtech.com.transform.DataTransformer;

import java.util.Map;

public class TransformationPipeline {
    private final PipelineConfig config;
    private final EncryptionService encryptionService;
    private final PubSubReader pubSubReader;
    private final FileProcessor fileProcessor;
    private final DataTransformer dataTransformer;
    private final BigQueryWriter bigQueryWriter;

    public TransformationPipeline(PipelineConfig config,
                                  EncryptionService encryptionService,
                                  PubSubReader pubSubReader,
                                  FileProcessor fileProcessor,
                                  DataTransformer dataTransformer,
                                  BigQueryWriter bigQueryWriter) {
        this.config = config;
        this.encryptionService = encryptionService;
        this.pubSubReader = pubSubReader;
        this.fileProcessor = fileProcessor;
        this.dataTransformer = dataTransformer;
        this.bigQueryWriter = bigQueryWriter;
    }

    public void run() {
        Pipeline pipeline = createPipeline();

        PCollection<String> messages = pubSubReader.readMessages(pipeline);
        PCollection<String> fileContents = fileProcessor.processFiles(messages);
        PCollection<Map<String, String>> parsedData = dataTransformer.parseAndValidate(fileContents);
        PCollection<Map<String, String>> encryptedData = encryptionService.encrypt(parsedData);
        PCollection<TableRow> tableRows = dataTransformer.convertToTableRows(encryptedData);

        bigQueryWriter.writeToTable(tableRows);

        pipeline.run().waitUntilFinish();
    }

    private Pipeline createPipeline() {
        return Pipeline.create(config.getPipelineOptions());
    }
}