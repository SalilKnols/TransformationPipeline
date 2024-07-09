package org.nashtech.com.pipeline;

import org.apache.beam.sdk.transforms.DoFn;
import org.nashtech.com.exceptions.EncryptionException;
import org.nashtech.com.util.AESUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EncryptDataFn extends DoFn<List<String>, List<String>> {
    private final Logger logger = LoggerFactory.getLogger(EncryptDataFn.class);
    private final List<String> schema;
    private final List<String> personalInfoColumns;
    private final String kmsKeyUri;
    private boolean isFirstRow = true;

    public EncryptDataFn(List<String> schema, List<String> personalInfoColumns, String kmsKeyUri) {
        this.schema = schema;
        this.personalInfoColumns = personalInfoColumns;
        this.kmsKeyUri = kmsKeyUri;
    }

    @ProcessElement
    public void processElement(@Element List<String> row, OutputReceiver<List<String>> out) {
        if (isFirstRow) {
            isFirstRow = false;
            return;
        }

        List<String> encryptedRow = IntStream.range(0, row.size())
                .mapToObj(index -> {
                    String columnName = schema.get(index);
                    try {
                        String encryptedValue = personalInfoColumns.contains(columnName) ?
                                AESUtil.encrypt(row.get(index), kmsKeyUri) : row.get(index);
                        logger.info("Column '{}' encrypted successfully", columnName);
                        return encryptedValue;
                    } catch (Exception encryptionException) {
                        logger.error("Encryption failed for column: {}", columnName, encryptionException);
                        throw new EncryptionException("Encryption failed for column: " + columnName, encryptionException);
                    }
                })
                .collect(Collectors.toList());

        logger.info("Encrypted row: {}", encryptedRow);
        out.output(encryptedRow);
    }
}
