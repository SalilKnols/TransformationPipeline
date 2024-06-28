package org.nashtech.com.config;

/**
 * Constants class holding cryptographic and file-related constants.
 */
public class Constants {

    /**
     * AES algorithm name.
     */
    public static final String AES_ALGORITHM = "AES";

    /**
     * AES transformation mode with GCM (Galois/Counter Mode) and no padding.
     */
    public static final String AES_TRANSFORMATION = "AES/GCM/NoPadding";

    /**
     * AES encryption key size (in bits).
     */
    public static final int AES_KEY_SIZE = 256;

    /**
     * Size of the initialization vector (IV) used in AES/GCM mode (in bytes).
     */
    public static final int IV_SIZE = 12;

    /**
     * Size of the authentication tag used in AES/GCM mode (in bits).
     */
    public static final int TAG_SIZE = 128;

    /**
     * RSA algorithm name.
     */
    public static final String RSA_ALGORITHM = "RSA";

    /**
     * RSA key size (in bits).
     */
    public static final int RSA_KEY_SIZE = 2048;

    /**
     * Name of the properties file containing configuration data.
     */
    public static final String PROPERTIES_FILE = "field.properties";
}
