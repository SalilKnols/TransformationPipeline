package org.nashtech.com.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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

public class PipelineSetup {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(PipelineSetup.class);
        PipelineOptions options = PipelineOptionsFactory.create();

        Properties prop = new Properties();
        try (InputStream input = PipelineSetup.class.getClassLoader().getResourceAsStream("field.properties")) {
            if (input == null) {
                System.err.println("Unable to find field.properties");
                return;
            }
            prop.load(input);
        } catch (IOException ioException) {
            ioException.printStackTrace();
            return;
        }

        String inputCsvFile = prop.getProperty("inputCsvFile");
        String outputCsvFile = prop.getProperty("outputCsvFile");
        List<String> schema = Arrays.asList(prop.getProperty("schema").split(","));
        List<String> personalInfoColumns = Arrays.asList(prop.getProperty("personalInfoColumns").split(","));

        try {
            SecretKey aesKey = AESUtil.generateAESKey();
            KeyPair rsaKeyPair = RSAUtil.generateRSAKeyPair();
            String encryptedAesKey = RSAUtil.encryptAESKeyWithRSA(aesKey, rsaKeyPair.getPublic());

            Pipeline pipeline = Pipeline.create(options);

            pipeline.apply("Read CSV", TextIO.read().from(inputCsvFile).withHintMatchesManyFiles())
                    .apply("Parse and Validate CSV", ParDo.of(new ParseAndValidateCsvFn(schema)))
                    .apply("Encrypt Data", ParDo.of(new EncryptDataFn(schema, personalInfoColumns, aesKey)))
                    .apply("Format CSV", MapElements.into(TypeDescriptor.of(String.class))
                            .via((List<String> row) -> String.join(",", row)))
                    .apply("Write CSV", TextIO.write().to(outputCsvFile).withSuffix(".csv").withoutSharding());

            pipeline.run().waitUntilFinish();

            logger.info("Encrypted AES Key: {}", encryptedAesKey);

        } catch (Exception encryptionException) {
            encryptionException.printStackTrace();
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
        private static final Logger logger = LoggerFactory.getLogger(EncryptDataFn.class);
        private final List<String> schema;
        private final List<String> personalInfoColumns;
        private final SecretKey aesKey;
        private boolean isFirstRow = true; // Flag to track if it's the first row

        EncryptDataFn(List<String> schema, List<String> personalInfoColumns, SecretKey aesKey) {
            this.schema = schema;
            this.personalInfoColumns = personalInfoColumns;
            this.aesKey = aesKey;
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
                            return personalInfoColumns.contains(columnName) ? AESUtil.encrypt(row.get(i), aesKey) : row.get(i);
                        } catch (Exception encryptionException) {
                            logger.error("Encryption failed", encryptionException);
                            throw new EncryptionException("Encryption failed for column: " + columnName, encryptionException);
                        }
                    })
                    .collect(Collectors.toList());
            out.output(encryptedRow);
        }
    }

}
