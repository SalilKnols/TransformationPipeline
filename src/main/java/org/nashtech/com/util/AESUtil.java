package org.nashtech.com.util;

import org.nashtech.com.config.Constants;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Utility class for AES encryption operations.
 */
public class AESUtil {

    /**
     * Generates a new AES encryption key.
     *
     * @return SecretKey object representing the generated AES key.
     * @throws Exception if there's an error during key generation.
     */
    public static SecretKey generateAESKey() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance(Constants.AES_ALGORITHM);
        keyGen.init(Constants.AES_KEY_SIZE);
        return keyGen.generateKey();
    }

    /**
     * Encrypts the given data using AES encryption with the provided AES key.
     *
     * @param data   The data to be encrypted.
     * @param aesKey The AES key used for encryption.
     * @return Base64-encoded string representation of the encrypted data with IV prepended.
     * @throws Exception if there's an error during encryption.
     */
    public static String encrypt(String data, SecretKey aesKey) throws Exception {
        Cipher cipher = Cipher.getInstance(Constants.AES_TRANSFORMATION);
        byte[] iv = new byte[Constants.IV_SIZE];
        SecureRandom random = new SecureRandom();
        random.nextBytes(iv);
        GCMParameterSpec spec = new GCMParameterSpec(Constants.TAG_SIZE, iv);
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);
        byte[] encrypted = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
        byte[] encryptedIvAndText = new byte[Constants.IV_SIZE + encrypted.length];
        System.arraycopy(iv, 0, encryptedIvAndText, 0, Constants.IV_SIZE);
        System.arraycopy(encrypted, 0, encryptedIvAndText, Constants.IV_SIZE, encrypted.length);
        return Base64.getEncoder().encodeToString(encryptedIvAndText);
    }
}
