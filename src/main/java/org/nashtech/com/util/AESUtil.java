package org.nashtech.com.util;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import org.nashtech.com.exceptions.EncryptionException;
import org.nashtech.com.exceptions.KeyGenerationException;
import org.nashtech.com.config.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class AESUtil {
    private static final Logger logger = LoggerFactory.getLogger(AESUtil.class);

    /**
     * Encrypts data using Google Cloud KMS.
     *
     * @param data      The data to encrypt
     * @param kmsKeyUri The URI of the KMS key to use for encryption
     * @return Base64 encoded ciphertext
     * @throws EncryptionException if encryption fails
     */
    public static String encrypt(String data, String kmsKeyUri) {
        try {
            KmsClient kmsClient = new GcpKmsClient().withCredentials("/home/nashtech/Documents/TransformationPipeline/src/main/resources/vernal-verve-428206-h2-195a74e70516.json");

            logger.info("Using service account credentials for AES encryption");

            Aead aead = kmsClient.getAead(kmsKeyUri);

            // Encrypt data using Tink's Aead interface
            byte[] ciphertext = aead.encrypt(data.getBytes(StandardCharsets.UTF_8), /* associatedData= */ new byte[0]);
            return Base64.getEncoder().encodeToString(ciphertext);
        } catch (GeneralSecurityException securityException) {
            logger.error("Encryption failed", securityException);
            throw new EncryptionException("Failed to encrypt data using KMS", securityException);
        }
    }

    /**
     * Generates an AES key.
     *
     * @return SecretKey object containing the generated AES key.
     * @throws KeyGenerationException if AES key generation fails
     */
    public static SecretKey generateAESKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance(Constants.AES_ALGORITHM);
            keyGen.init(Constants.AES_KEY_SIZE);
            return keyGen.generateKey();
        } catch (NoSuchAlgorithmException algorithmException) {
            logger.error("AES key generation failed", algorithmException);
            throw new KeyGenerationException("Failed to generate AES key", algorithmException);
        }
    }
}
