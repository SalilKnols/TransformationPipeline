package org.nashtech.com;

import org.nashtech.com.config.PipelineConfig;
import org.nashtech.com.io.BigQueryWriter;
import org.nashtech.com.io.FileProcessor;
import org.nashtech.com.io.PubSubReader;
import org.nashtech.com.pipeline.TransformationPipeline;
import org.nashtech.com.service.EncryptionService;
import org.nashtech.com.transform.DataTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            PipelineConfig config = new PipelineConfig("field.properties");
            EncryptionService encryptionService = new EncryptionService(config);
            PubSubReader pubSubReader = new PubSubReader(config.getPubsubSubscription());
            FileProcessor fileProcessor = new FileProcessor();
            DataTransformer dataTransformer = new DataTransformer(config.getSchema());
            BigQueryWriter bigQueryWriter = new BigQueryWriter(config);

            TransformationPipeline pipeline = new TransformationPipeline(
                    config, encryptionService, pubSubReader, fileProcessor,
                    dataTransformer, bigQueryWriter);

            pipeline.run();

        } catch (Exception e) {
            logger.error("An error occurred during pipeline execution", e);
        }
    }
}