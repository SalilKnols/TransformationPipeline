package com.nashtech.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptor;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import java.util.Base64;

public class EncryptCSV{

    private static final String AES_ALGORITHM = "AES";
    private static final String AES_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int AES_KEY_SIZE = 256;
    private static final int IV_SIZE = 12;
    private static final int TAG_SIZE = 128;

    private static final String RSA_ALGORITHM = "RSA";
    private static final int RSA_KEY_SIZE = 2048;

    public static void main(String[] args) {
        String inputCsvFile = "/home/nashtech/Downloads/customers-100.csv";
        String outputCsvFile = "/home/nashtech/Downloads/encrypted.csv";
        List<String> schema = Arrays.asList("Index","Customer Id","First Name","Last Name","City","Country","Phone 1","Phone 2","Email","Subscription Date","Website"
        );
        List<String> personalInfoColumns = Arrays.asList("Customer Id", "First Name", "Last Name", "City", "Phone 1", "Phone 2", "Email", "Website");

        try {
            SecretKey aesKey = generateAESKey();
            KeyPair rsaKeyPair = generateRSAKeyPair();

            String encryptedAesKey = encryptAESKeyWithRSA(aesKey, rsaKeyPair.getPublic());

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

    private static SecretKey generateAESKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance(AES_ALGORITHM);
        keyGen.init(AES_KEY_SIZE);
        return keyGen.generateKey();
    }

    private static KeyPair generateRSAKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance(RSA_ALGORITHM);
        keyPairGen.initialize(RSA_KEY_SIZE);
        return keyPairGen.generateKeyPair();
    }

    private static String encryptAESKeyWithRSA(SecretKey aesKey, PublicKey publicKey) throws Exception {
        Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        byte[] encryptedKey = cipher.doFinal(aesKey.getEncoded());
        return Base64.getEncoder().encodeToString(encryptedKey);
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
                    encryptedRow.add(encrypt(row.get(i), aesKey));
                } else {
                    encryptedRow.add(row.get(i));
                }
            }
            out.output(encryptedRow);
        }
    }

    private static String encrypt(String data, SecretKey aesKey) throws Exception {
        Cipher cipher = Cipher.getInstance(AES_TRANSFORMATION);
        byte[] iv = new byte[IV_SIZE];
        SecureRandom random = new SecureRandom();
        random.nextBytes(iv);
        GCMParameterSpec spec = new GCMParameterSpec(TAG_SIZE, iv);
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);
        byte[] encrypted = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
        byte[] encryptedIvAndText = new byte[IV_SIZE + encrypted.length];
        System.arraycopy(iv, 0, encryptedIvAndText, 0, IV_SIZE);
        System.arraycopy(encrypted, 0, encryptedIvAndText, IV_SIZE, encrypted.length);
        return Base64.getEncoder().encodeToString(encryptedIvAndText);
    }
}



//package com.nashtech.pipeline;
//
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVPrinter;
//import org.apache.commons.csv.CSVRecord;
//
//import javax.crypto.Cipher;
//import javax.crypto.KeyGenerator;
//import javax.crypto.SecretKey;
//import javax.crypto.SecretKeyFactory;
//import javax.crypto.spec.GCMParameterSpec;
//import javax.crypto.spec.PBEKeySpec;
//import javax.crypto.spec.SecretKeySpec;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.security.SecureRandom;
//import java.security.spec.KeySpec;
//import java.util.ArrayList;
//import java.util.Base64;
//import java.util.List;
//
//public class EncryptCSV {
//
//    private static final String ALGORITHM = "AES";
//    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
//    private static final int KEY_SIZE = 256;
//    private static final int IV_SIZE = 12;
//    private static final int TAG_SIZE = 128;
//    private static final int SALT_SIZE = 16;
//    private static final int ITERATIONS = 65536;
//
//    public static void main(String[] args) {
//        String inputCsvFile = "/home/nashtech/Downloads/customers-100.csv";
//        String outputCsvFile = "/home/nashtech/Downloads/encrypted.csv";
//        String password = "your-secret-password"; // Ensure this is securely stored and managed
//
//        try {
//            SecretKey key = deriveKeyFromPassword(password);
//            List<String[]> data = readCsv(inputCsvFile);
//            List<String[]> encryptedData = encryptData(data, key);
//            writeCsv(outputCsvFile, encryptedData);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static SecretKey deriveKeyFromPassword(String password) throws Exception {
//        byte[] salt = new byte[SALT_SIZE];
//        SecureRandom random = new SecureRandom();
//        random.nextBytes(salt);
//
//        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, ITERATIONS, KEY_SIZE);
//        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
//        byte[] keyBytes = factory.generateSecret(spec).getEncoded();
//
//        return new SecretKeySpec(keyBytes, ALGORITHM);
//    }
//
//    private static List<String[]> readCsv(String filePath) throws IOException {
//        List<String[]> data = new ArrayList<>();
//        try (FileReader reader = new FileReader(filePath)) {
//            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
//            for (CSVRecord record : records) {
//                int size = record.size();
//                String[] row = new String[size];
//                for (int i = 0; i < size; i++) {
//                    row[i] = record.get(i);
//                }
//                data.add(row);
//            }
//        }
//        return data;
//    }
//
//    private static List<String[]> encryptData(List<String[]> data, SecretKey key) throws Exception {
//        List<String[]> encryptedData = new ArrayList<>();
//        for (String[] row : data) {
//            String[] encryptedRow = new String[row.length];
//            for (int i = 0; i < row.length; i++) {
//                encryptedRow[i] = encrypt(row[i], key);
//            }
//            encryptedData.add(encryptedRow);
//        }
//        return encryptedData;
//    }
//
//    private static String encrypt(String data, SecretKey key) throws Exception {
//        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
//        byte[] iv = new byte[IV_SIZE];
//        SecureRandom random = new SecureRandom();
//        random.nextBytes(iv);
//        GCMParameterSpec spec = new GCMParameterSpec(TAG_SIZE, iv);
//        cipher.init(Cipher.ENCRYPT_MODE, key, spec);
//        byte[] encrypted = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
//        byte[] encryptedIvAndText = new byte[IV_SIZE + encrypted.length];
//        System.arraycopy(iv, 0, encryptedIvAndText, 0, IV_SIZE);
//        System.arraycopy(encrypted, 0, encryptedIvAndText, IV_SIZE, encrypted.length);
//        return Base64.getEncoder().encodeToString(encryptedIvAndText);
//    }
//
//    private static void writeCsv(String filePath, List<String[]> data) throws IOException {
//        try (FileWriter writer = new FileWriter(filePath);
//             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
//            for (String[] row : data) {
//                csvPrinter.printRecord((Object[]) row);
//            }
//        }
//    }
//}
//



//package com.nashtech.pipeline;
//
//import org.apache.commons.csv.CSVFormat;



//import org.apache.commons.csv.CSVPrinter;
//import org.apache.commons.csv.CSVRecord;
//
//import javax.crypto.Cipher;
//import javax.crypto.KeyGenerator;
//import javax.crypto.SecretKey;
//import javax.crypto.spec.GCMParameterSpec;
//import javax.crypto.spec.SecretKeySpec;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.security.SecureRandom;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Base64;
//
//public class EncryptCSV {
//
//    private static final String ALGORITHM = "AES";
//    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
//    private static final int KEY_SIZE = 256;
//    private static final int IV_SIZE = 12;
//    private static final int TAG_SIZE = 128;
//    private static final int SALT_SIZE = 16;
//    private static final int ITERATIONS = 65536;
//
//    public static void main(String[] args) {
//        String inputCsvFile = "/home/nashtech/Downloads/customers-100.csv";
//        String outputCsvFile = "/home/nashtech/Downloads/encrypted.csv";
//        String secretKey = "salilvermachutiyahai"; // Ensure this is securely stored and managed
//
//        try {
//            SecretKey key = generateKey(secretKey);
//            List<String[]> data = readCsv(inputCsvFile);
//            List<String[]> encryptedData = encryptData(data, key);
//            writeCsv(outputCsvFile, encryptedData);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static SecretKey generateKey(String secret) throws Exception {
//        byte[] keyBytes = secret.getBytes(StandardCharsets.UTF_8);
//        return new SecretKeySpec(keyBytes, ALGORITHM);
//    }
//
//    private static List<String[]> readCsv(String filePath) throws IOException {
//        List<String[]> data = new ArrayList<>();
//        try (FileReader reader = new FileReader(filePath)) {
//            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
//            for (CSVRecord record : records) {
//                int size = record.size();
//                String[] row = new String[size];
//                for (int i = 0; i < size; i++) {
//                    row[i] = record.get(i);
//                }
//                data.add(row);
//            }
//        }
//        return data;
//    }
//
//    private static List<String[]> encryptData(List<String[]> data, SecretKey key) throws Exception {
//        List<String[]> encryptedData = new ArrayList<>();
//        for (String[] row : data) {
//            String[] encryptedRow = new String[row.length];
//            for (int i = 0; i < row.length; i++) {
//                encryptedRow[i] = encrypt(row[i], key);
//            }
//            encryptedData.add(encryptedRow);
//        }
//        return encryptedData;
//    }
//
//    private static String encrypt(String data, SecretKey key) throws Exception {
//        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
//        byte[] iv = new byte[IV_SIZE];
//        SecureRandom random = new SecureRandom();
//        random.nextBytes(iv);
//        GCMParameterSpec spec = new GCMParameterSpec(TAG_SIZE, iv);
//        cipher.init(Cipher.ENCRYPT_MODE, key, spec);
//        byte[] encrypted = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
//        byte[] encryptedIvAndText = new byte[IV_SIZE + encrypted.length];
//        System.arraycopy(iv, 0, encryptedIvAndText, 0, IV_SIZE);
//        System.arraycopy(encrypted, 0, encryptedIvAndText, IV_SIZE, encrypted.length);
//        return Base64.getEncoder().encodeToString(encryptedIvAndText);
//    }
//
//    private static void writeCsv(String filePath, List<String[]> data) throws IOException {
//        try (FileWriter writer = new FileWriter(filePath);
//             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
//            for (String[] row : data) {
//                csvPrinter.printRecord((Object[]) row);
//            }
//        }
//    }
//}
