package org.nashtech.com.pipeline;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.nashtech.com.exceptions.EncryptionException;
import org.nashtech.com.util.AESUtil;
import org.nashtech.com.util.BigQueryUtil;
import org.nashtech.com.util.RSAUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TransformationPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TransformationPipeline.class);

    public static void main(String[] args) {
        Properties prop = new Properties();
        try (InputStream input = TransformationPipeline.class.getClassLoader().getResourceAsStream("field.properties")) {
            if (input == null) {
                logger.error("Unable to find field.properties");
                return;
            }
            prop.load(input);
        } catch (IOException ioException) {
            logger.error("Failed to load field.properties", ioException);
            return;
        }

        String inputCsvFile = prop.getProperty("inputCsvFile");
        String bqTable = prop.getProperty("bqTable");
        String bqProject = prop.getProperty("bqProject");
        String bqDataset = prop.getProperty("bqDataset");
        List<String> schema = Arrays.asList(prop.getProperty("schema").split(","));
        List<String> personalInfoColumns = Arrays.asList(prop.getProperty("personalInfoColumns").split(","));
        String kmsKeyUri = prop.getProperty("kmsKeyUri");
        String tempLocation = prop.getProperty("tempLocation");

        PipelineOptions options = PipelineOptionsFactory.create();
        options.setTempLocation(tempLocation);

        try {
            SecretKey aesKey = AESUtil.generateAESKey();
            KeyPair rsaKeyPair = RSAUtil.generateRSAKeyPair();
            String encryptedAesKey = RSAUtil.encryptAESKeyWithRSA(aesKey, rsaKeyPair.getPublic());

            Pipeline pipeline = Pipeline.create(options);

            pipeline.apply("Read CSV", TextIO.read().from(inputCsvFile).withHintMatchesManyFiles())
                    .apply("Parse and Validate CSV", ParDo.of(new ParseAndValidateCsvFn(schema)))
                    .apply("Encrypt Data", ParDo.of(new EncryptDataFn(schema, personalInfoColumns, kmsKeyUri)))
                    .apply("Format CSV", MapElements.into(TypeDescriptor.of(String.class))
                            .via((List<String> row) -> String.join(",", row)))
                    .apply("Convert to TableRow", MapElements.into(TypeDescriptor.of(TableRow.class))
                            .via(csvLine -> {
                                TableRow tableRow = new TableRow();
                                String[] values = csvLine.split(",");
                                IntStream.range(0, values.length)
                                        .forEach(index -> tableRow.set(schema.get(index), values[index]));
                                return tableRow;
                            }))
                    .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                            .to(String.format("%s:%s.%s", bqProject, bqDataset, bqTable))
                            .withSchema(BigQueryUtil.createBigQuerySchema(schema))
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(tempLocation)));

            pipeline.run().waitUntilFinish();

            logger.info("Encrypted AES Key: {}", encryptedAesKey);

        } catch (Exception encryptionException) {
            logger.error("Pipeline execution failed", encryptionException);
        }
    }
}
