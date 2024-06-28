package org.nashtech.com.pipeline;

import org.nashtech.com.util.AESUtil;
import org.nashtech.com.util.RSAUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPair;
import java.util.*;

/**
 * DataTransformation class handles the transformation pipeline for encrypting CSV data
 * and writing it to a new CSV file.
 */
public class DataTransformation {

    /**
     * Main method for executing the data transformation pipeline.
     * @param args Command-line arguments (not used in this implementation)
     */
    public static void main(String[] args) {
        Properties prop = new Properties();
        try (InputStream input = DataTransformation.class.getClassLoader().getResourceAsStream("field.properties")) {
            if (input == null) {
                System.err.println("Unable to find field.properties");
                return;
            }
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
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

            Pipeline p = Pipeline.create();

            p.apply("Read CSV", TextIO.read().from(inputCsvFile))
                    .apply("Parse and Validate CSV", ParDo.of(new ParseAndValidateCsvFn(schema)))
                    .apply("Encrypt Data", ParDo.of(new EncryptDataFn(schema, personalInfoColumns, aesKey)))
                    .apply("Format CSV", MapElements.into(TypeDescriptor.of(String.class))
                            .via((List<String> row) -> String.join(",", row)))
                    .apply("Write CSV", TextIO.write().to(outputCsvFile).withSuffix(".csv").withoutSharding());

            p.run().waitUntilFinish();

            // Print or save the encrypted AES key (for demonstration purposes)
            System.out.println("Encrypted AES Key: " + encryptedAesKey);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * DoFn class for parsing and validating CSV input based on a given schema.
     */
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

    /**
     * DoFn class for encrypting sensitive data in CSV rows based on specified columns.
     */
    static class EncryptDataFn extends DoFn<List<String>, List<String>> {
        private final List<String> schema;
        private final List<String> personalInfoColumns;
        private final SecretKey aesKey;

        EncryptDataFn(List<String> schema, List<String> personalInfoColumns, SecretKey aesKey) {
            this.schema = schema;
            this.personalInfoColumns = personalInfoColumns;
            this.aesKey = aesKey;
        }

        @ProcessElement
        public void processElement(@Element List<String> row, OutputReceiver<List<String>> out) throws Exception {
            List<String> encryptedRow = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                String columnName = schema.get(i);
                if (personalInfoColumns.contains(columnName)) {
                    encryptedRow.add(AESUtil.encrypt(row.get(i), aesKey));
                } else {
                    encryptedRow.add(row.get(i));
                }
            }
            out.output(encryptedRow);
        }
    }
}
