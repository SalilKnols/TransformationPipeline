package org.nashtech.com.util;

import com.google.crypto.tink.signature.SignatureConfig;
import org.nashtech.com.exceptions.EncryptionException;
import org.nashtech.com.exceptions.KeyGenerationException;
import org.nashtech.com.config.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Base64;

public class RSAUtil {
    private static final Logger logger = LoggerFactory.getLogger(RSAUtil.class);

    static {
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
            KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance(Constants.RSA_ALGORITHM);
            keyPairGen.initialize(Constants.RSA_KEY_SIZE);
            KeyPair keyPair = keyPairGen.generateKeyPair();
            logger.info("RSA key pair generated successfully");
            return keyPair;
        } catch (Exception keyGenerationException) {
            logger.error("RSA key pair generation failed", keyGenerationException);
            throw new KeyGenerationException("Failed to generate RSA key pair", keyGenerationException);
        }
    }

    /**
     * Encrypts an AES key using RSA public key.
     *
     * @param aesKey AES key to encrypt
     * @param publicKey  RSA public key
     * @return Base64 encoded encrypted AES key
     * @throws EncryptionException if encryption fails
     */
    public static String encryptAESKeyWithRSA(SecretKey aesKey, PublicKey publicKey) {
        try {
            Cipher cipher = Cipher.getInstance(Constants.RSA_ALGORITHM + "/ECB/PKCS1Padding");
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            byte[] encryptedKey = cipher.doFinal(aesKey.getEncoded());
            return Base64.getEncoder().encodeToString(encryptedKey);
        } catch (Exception encryptionException) {
            logger.error("AES key encryption with RSA failed", encryptionException);
            throw new EncryptionException("Failed to encrypt AES key with RSA", encryptionException);
        }
    }
}
