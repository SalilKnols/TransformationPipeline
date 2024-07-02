package org.nashtech.com.util;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AESUtil {
    private static final Logger logger = LoggerFactory.getLogger(AESUtil.class);

    /**
     * Encrypts data using Google Cloud KMS.
     *
     * @param data     The data to encrypt
     * @param kmsKeyUri The URI of the KMS key to use for encryption
     * @return Base64 encoded ciphertext
     * @throws GeneralSecurityException if encryption fails
     */
    public static String encrypt(String data, String kmsKeyUri) throws Exception {
        try {
            // Create a KmsClient using the provided credentials path
            KmsClient kmsClient = new GcpKmsClient().withCredentials("/home/nashtech/Documents/TransformationPipeline/src/main/resources/vernal-verve-428206-h2-23df7cd24ebc.json");

            // Log successful service account usage
            logger.info("Using service account credentials for AES encryption");

            // Get an Aead primitive using the KMS key URI
            Aead aead = kmsClient.getAead(kmsKeyUri);

            // Encrypt data using Tink's Aead interface
            byte[] ciphertext = aead.encrypt(data.getBytes(StandardCharsets.UTF_8), /* associatedData= */ new byte[0]);
            return Base64.getEncoder().encodeToString(ciphertext);
        } catch (GeneralSecurityException e) {
            logger.error("Encryption failed", e);
            throw e;
        }
    }

    public static SecretKey generateAESKey() throws NoSuchAlgorithmException {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256); // Choose key size as per your requirements
            return keyGen.generateKey();
        } catch (NoSuchAlgorithmException e) {
            logger.error("AES key generation failed", e);
            throw e;
        }
    }
}
