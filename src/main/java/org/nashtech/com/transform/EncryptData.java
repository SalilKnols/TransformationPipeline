package org.nashtech.com.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.nashtech.com.exceptions.EncryptionException;
import org.nashtech.com.util.AESUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EncryptData extends DoFn<Map<String, String>, Map<String, String>> {
    private final Logger logger = LoggerFactory.getLogger(EncryptData.class);
    private final List<String> schema;
    private final List<String> personalInfoColumns;
    private final String kmsKeyUri;

    public EncryptData(List<String> schema, List<String> personalInfoColumns, String kmsKeyUri) {
        this.schema = schema;
        this.personalInfoColumns = personalInfoColumns;
        this.kmsKeyUri = kmsKeyUri;
    }

    @ProcessElement
    public void processElement(@Element Map<String, String> row, OutputReceiver<Map<String, String>> out) {
        try {
            Map<String, String> encryptedRow = new HashMap<>(row); // Create a new map for encrypted values

            for (String column : personalInfoColumns) {
                if (row.containsKey(column)) {
                    String encryptedValue = AESUtil.encrypt(row.get(column), kmsKeyUri);
                    encryptedRow.put(column, encryptedValue);
                    logger.info("Column '{}' encrypted successfully", column);
                }
            }

            logger.info("Encrypted row: {}", encryptedRow);
            out.output(encryptedRow); // Output the new map containing encrypted values
        } catch (Exception encryptionException) {
            logger.error("Encryption failed", encryptionException);
            throw new EncryptionException("Encryption failed", encryptionException);
        }
    }
}
