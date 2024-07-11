package org.nashtech.com;

import com.google.cloud.secretmanager.v1.*;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.protobuf.ByteString;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Properties;

public class TransformationPipelineWithSecretKey {

    private static final Logger logger = LoggerFactory.getLogger(TransformationPipelineWithSecretKey.class);

    public static void main(String[] args) {

        Properties prop = new Properties();
        String PROJECT_ID = prop.getProperty("PROJECT_ID");
        String SECRET_ID = prop.getProperty("SECRET_ID");
        String SECRET_VERSION = prop.getProperty("SECRET_VERSION");
        String inputFile = prop.getProperty("inputFile");
        String outputFile = prop.getProperty("outputFile");
        String encryptedFilePath = prop.getProperty("encryptedFilePath");
        String decryptedFilePath = prop.getProperty("decryptedFilePath");

        // Initialize Tink
        try {
            AeadConfig.register();
        } catch (GeneralSecurityException exception) {
           logger.info("Error during Tink initialization:",exception.getMessage());
            return;
        }
        // Create the Pipeline
        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        options.setRunner(DirectRunner.class);
        Pipeline p = Pipeline.create(options);

        // Create a PCollection with a single element (dummy element to trigger the pipeline)
        PCollection<String> dummyCollection = p.apply("Create Dummy", Create.of("dummy"));

       /* *//**
         * Step 1: Generate, Encode, and Store Key through GCP KMS using TINK library.
         *//*
        dummyCollection.apply("Generate, Encode, and Store Key", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                try {
                    // Generate a new keyset handle
                    KeysetHandle keysetHandle = KeysetHandle.generateNew(AeadKeyTemplates.AES256_GCM);

                    // Extract the key material
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    CleartextKeysetHandle.write(keysetHandle, JsonKeysetWriter.withOutputStream(outputStream));
                    String keyJson = outputStream.toString();

                    // Encode the key in Base64
                    String base64Key = Base64.getEncoder().encodeToString(keyJson.getBytes());

                    // Store the Base64 encoded key in Google Secret Manager
                    storeKeyInSecretManager(PROJECT_ID, SECRET_ID, base64Key);
                } catch (Exception exception) {
                    logger.error("Failed to generate, encode, and store key in Secret manager", exception.getMessage());                }
            }
        }));*/


      /**
        * Step 2: Read CSV file, encrypt its contents, and write encrypted data to a new file.
        */
        dummyCollection.apply("Read CSV File and Encrypt", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
                try {
                    // Read CSV file content
                    byte[] fileContent = Files.readAllBytes(Paths.get(inputFile));

                        // Retrieve the secret version and Decode the Base64 encoded key
                        String base64Key = retrieveKeyFromSecretManager();
                        byte[] keyBytes = Base64.getDecoder().decode(base64Key);
                        String key = new String(keyBytes, StandardCharsets.UTF_8);

                        // Load keyset and get AEAD primitive
                        KeysetHandle keysetHandle = CleartextKeysetHandle.read(JsonKeysetReader.withBytes(keyBytes));
                        Aead aead = keysetHandle.getPrimitive(Aead.class);

                        // Encrypt the file content
                        byte[] encryptedData = aead.encrypt(fileContent, null);

                        // Write encrypted data to output file
                        try {
                            Files.write(Paths.get(outputFile), encryptedData);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

            }
        }));

        /**
         * Step 3: Retrieve data from secret manager, decode it using base64, and decrypting the encrypted file.
         */
        dummyCollection.apply("File Decryption", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
                String encryptionKey = c.element();

                try {

                    // Retrieve the secret version
                    try {
                        String base64Key = retrieveKeyFromSecretManager();
                        byte[] keyBytes = Base64.getDecoder().decode(base64Key);
                        String key = new String(keyBytes, StandardCharsets.UTF_8);

                        // Load keyset and get AEAD primitive
                        KeysetHandle keysetHandle = CleartextKeysetHandle.read(JsonKeysetReader.withBytes(keyBytes));
                        Aead aead = keysetHandle.getPrimitive(Aead.class);

                        // Perform decryption and write decrypted data
                        decryptCsvFile(encryptedFilePath, decryptedFilePath, keyBytes);
                        // Write encrypted data to output file
                        }catch (Exception exception) {
                       logger.error("Error while retrieving data from secret manager, decoding it using base64 and decrypting it.",exception.getMessage());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }));

        // Run the pipeline
        p.run().waitUntilFinish();
    }

    /**
     * Stores a Base64-encoded key in Google Secret Manager for a given project and secret ID.
     *
     * @param projectId  The ID of the Google Cloud project.
     * @param secretId   The ID of the secret within Secret Manager.
     * @param base64Key  The Base64-encoded key to store as a secret version.
     * @throws Exception If an error occurs while interacting with Secret Manager.
     */
    public static void storeKeyInSecretManager(String projectId, String secretId, String base64Key) throws Exception {
        SecretManagerServiceSettings settings = SecretManagerServiceSettings.newBuilder().build();
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create(settings)) {
            // Create the secret
            Secret secret = Secret.newBuilder()
                    .setReplication(Replication.newBuilder()
                            .setAutomatic(Replication.Automatic.getDefaultInstance()))
                    .build();
            CreateSecretRequest createSecretRequest = CreateSecretRequest.newBuilder()
                    .setParent("projects/" + projectId)
                    .setSecretId(secretId)
                    .setSecret(secret)
                    .build();
            client.createSecret(createSecretRequest);

            // Add the secret version
            SecretPayload payload = SecretPayload.newBuilder()
                    .setData(ByteString.copyFromUtf8(base64Key))
                    .build();
            AddSecretVersionRequest addSecretVersionRequest = AddSecretVersionRequest.newBuilder()
                    .setParent(SecretName.of(projectId, secretId).toString())
                    .setPayload(payload)
                    .build();
            client.addSecretVersion(addSecretVersionRequest);
        }
    }

    public interface MyOptions extends PipelineOptions {
        // Custom pipeline options if needed
    }

    /**
     * Retrieves a key from Google Secret Manager based on the configured project ID, secret ID, and secret version.
     *
     * @return The retrieved key as a UTF-8 encoded string.
     * @throws Exception If an error occurs while accessing Secret Manager or retrieving the key.
     */
    private static String retrieveKeyFromSecretManager() throws Exception {
        Properties prop = new Properties();
        String PROJECT_ID = prop.getProperty("PROJECT_ID");
        String SECRET_ID = prop.getProperty("SECRET_ID");
        String SECRET_VERSION = prop.getProperty("SECRET_VERSION");
        SecretManagerServiceSettings settings = SecretManagerServiceSettings.newBuilder().build();
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create(settings)) {
            String secretVersionName = SecretVersionName.of(PROJECT_ID, SECRET_ID, SECRET_VERSION).toString();
            SecretPayload payload = client.accessSecretVersion(secretVersionName).getPayload();
            return payload.getData().toStringUtf8();
        }
    }

    /**
     * Decrypts an encrypted CSV file using a provided key and writes the decrypted content to a new file.
     *
     * @param encryptedFilePath The path to the encrypted CSV file.
     * @param decryptedFilePath The path to write the decrypted CSV content.
     * @param keyBytes          The key used for decryption as a byte array.
     * @throws Exception If an error occurs during keyset loading, decryption, or file operations.
     */
    private static void decryptCsvFile(String encryptedFilePath, String decryptedFilePath, byte[] keyBytes) throws Exception {
        // Loading the keyset.
        KeysetHandle keysetHandle = CleartextKeysetHandle.read(JsonKeysetReader.withBytes(keyBytes));

        // Get the AEAD primitive.
        Aead aead = keysetHandle.getPrimitive(Aead.class);

        // Read the encrypted CSV file content.
        byte[] encryptedData = Files.readAllBytes(Paths.get(encryptedFilePath));

        // Decrypt the CSV file content.
        byte[] decryptedData = aead.decrypt(encryptedData, null);

        // Write the decrypted data to an output file.
        try (FileOutputStream outputStream = new FileOutputStream(decryptedFilePath)) {
            outputStream.write(decryptedData);
        } catch (Exception exception){
            logger.error("Error while writing the decrypted data to an output file:",exception.getMessage());
        }
    }
}
