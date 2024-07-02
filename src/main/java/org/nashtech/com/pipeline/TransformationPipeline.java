package org.nashtech.com.pipeline;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.nashtech.com.exceptions.EncryptionException;
import org.nashtech.com.util.AESUtil;
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
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setTempLocation("gs://nashtechbeam");

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
        String outputCsvFile = prop.getProperty("outputCsvFile");
        String bqTable = prop.getProperty("bqTable"); // BigQuery table name
        String bqProject = prop.getProperty("bqProject"); // BigQuery project ID
        String bqDataset = prop.getProperty("bqDataset"); // BigQuery dataset ID
        List<String> schema = Arrays.asList(prop.getProperty("schema").split(","));
        List<String> personalInfoColumns = Arrays.asList(prop.getProperty("personalInfoColumns").split(","));
        String kmsKeyUri = prop.getProperty("kmsKeyUri");

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
                            .via((String csvLine) -> {
                                TableRow row = new TableRow();
                                String[] values = csvLine.split(",");
                                for (int i = 0; i < values.length; i++) {
                                    row.set(schema.get(i), values[i]);
                                }
                                return row;
                            }))
                    .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                            .to(String.format("%s:%s.%s", bqProject, bqDataset, bqTable))
                            .withSchema(BigQueryUtil.createBigQuerySchema(schema)) // Create schema dynamically
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of("gs://nashtechbeam"))); // Specify GCS temp location
// Specify GCS temp location

            pipeline.run().waitUntilFinish();

            logger.info("Encrypted AES Key: {}", encryptedAesKey);

        } catch (Exception encryptionException) {
            logger.error("Pipeline execution failed", encryptionException);
        }
    }

    static class ParseAndValidateCsvFn extends DoFn<String, List<String>> {
        private final List<String> schema;

        ParseAndValidateCsvFn(List<String> schema) {
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

    static class EncryptDataFn extends DoFn<List<String>, List<String>> {
        private final Logger logger = LoggerFactory.getLogger(EncryptDataFn.class);
        private final List<String> schema;
        private final List<String> personalInfoColumns;
        private final String kmsKeyUri;
        private boolean isFirstRow = true;

        EncryptDataFn(List<String> schema, List<String> personalInfoColumns, String kmsKeyUri) {
            this.schema = schema;
            this.personalInfoColumns = personalInfoColumns;
            this.kmsKeyUri = kmsKeyUri;
        }

        @ProcessElement
        public void processElement(@Element List<String> row, OutputReceiver<List<String>> out) {
            if (isFirstRow) {
                out.output(row);
                isFirstRow = false;
                return;
            }

            List<String> encryptedRow = IntStream.range(0, row.size())
                    .mapToObj(i -> {
                        String columnName = schema.get(i);
                        try {
                            String encryptedValue = personalInfoColumns.contains(columnName) ?
                                    AESUtil.encrypt(row.get(i), kmsKeyUri) : row.get(i);
                            logger.info("Column '{}' encrypted successfully", columnName);
                            return encryptedValue;
                        } catch (Exception encryptionException) {
                            logger.error("Encryption failed for column: {}", columnName, encryptionException);
                            throw new EncryptionException("Encryption failed for column: " + columnName, encryptionException);
                        }
                    })
                    .collect(Collectors.toList());

            logger.info("Encrypted row: {}", encryptedRow);
            out.output(encryptedRow);
        }
    }

    static class BigQueryUtil {
        static TableSchema createBigQuerySchema(List<String> schema) {
            List<TableFieldSchema> fields = schema.stream()
                    .map(fieldName -> new TableFieldSchema().setName(fieldName).setType("STRING"))
                    .collect(Collectors.toList());
            return new TableSchema().setFields(fields);
        }
    }
}
