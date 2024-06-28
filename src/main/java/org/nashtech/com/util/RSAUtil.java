package org.nashtech.com.util;

import org.nashtech.com.config.Constants;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Base64;

/**
 * Utility class for RSA encryption operations.
 */
public class RSAUtil {

    /**
     * Generates an RSA key pair.
     *
     * @return KeyPair object containing the generated public and private keys.
     * @throws Exception if there's an error during key pair generation.
     */
    public static KeyPair generateRSAKeyPair() throws Exception {
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance(Constants.RSA_ALGORITHM);
        keyPairGen.initialize(Constants.RSA_KEY_SIZE);
        return keyPairGen.generateKeyPair();
    }

    /**
     * Encrypts a given AES key using RSA with the provided public key.
     *
     * @param aesKey    The AES key to be encrypted.
     * @param publicKey The public key used for encryption.
     * @return Base64-encoded string representation of the encrypted AES key.
     * @throws Exception if there's an error during encryption.
     */
    public static String encryptAESKeyWithRSA(SecretKey aesKey, PublicKey publicKey) throws Exception {
        Cipher cipher = Cipher.getInstance(Constants.RSA_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        byte[] encryptedKey = cipher.doFinal(aesKey.getEncoded());
        return Base64.getEncoder().encodeToString(encryptedKey);
    }
}
