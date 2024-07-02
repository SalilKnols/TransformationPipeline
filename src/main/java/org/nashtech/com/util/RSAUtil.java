package org.nashtech.com.util;

import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.PublicKeySign;
import com.google.crypto.tink.signature.SignatureConfig;
import org.nashtech.com.exceptions.EncryptionException;
import org.nashtech.com.exceptions.KeyGenerationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Base64;

public class RSAUtil {
    private static final Logger logger = LoggerFactory.getLogger(RSAUtil.class);

    static {
        // Initialize Tink for RSA signature operations
        try {
            SignatureConfig.register();
        } catch (Exception initializationException) {
            logger.error("Failed to initialize Tink for RSA operations", initializationException);
            throw new ExceptionInInitializerError(initializationException);
        }
    }

    /**
     * Generates an RSA key pair using Java's KeyPairGenerator.
     *
     * @return KeyPair object containing the generated public and private keys.
     * @throws KeyGenerationException if there's an error during key pair generation.
     */
    public static KeyPair generateRSAKeyPair() {
        try {
            KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
            keyPairGen.initialize(2048); // You can adjust the key size as needed
            KeyPair keyPair = keyPairGen.generateKeyPair();
            logger.info("RSA key pair generated successfully");
            return keyPair;
        } catch (Exception keyGenerationException) {
            logger.error("RSA key pair generation failed", keyGenerationException);
            throw new KeyGenerationException("Failed to generate RSA key pair", keyGenerationException);
        }
    }

    /**
     * Encrypts a given AES key using RSA with the provided public key.
     *
     * @param aesKey    The AES key to be encrypted.
     * @param publicKey The public key used for encryption.
     * @return Base64-encoded string representation of the encrypted AES key.
     * @throws EncryptionException if there's an error during encryption.
     */
    public static String encryptAESKeyWithRSA(SecretKey aesKey, PublicKey publicKey) {
        try {
            // Get an instance of Tink's RSA encryption primitive
            KeysetHandle privateKeysetHandle = KeysetHandle.generateNew(KeyTemplates.get("RSA_SSA_PKCS1_4096_SHA512_F4"));
            PublicKeySign signer = privateKeysetHandle.getPrimitive(PublicKeySign.class);

            // Encrypt the AES key using RSA
            byte[] encryptedKey = signer.sign(aesKey.getEncoded());
            return Base64.getEncoder().encodeToString(encryptedKey);
        } catch (Exception encryptionException) {
            logger.error("AES key encryption with RSA failed", encryptionException);
            throw new EncryptionException("Failed to encrypt AES key with RSA", encryptionException);
        }
    }
}
