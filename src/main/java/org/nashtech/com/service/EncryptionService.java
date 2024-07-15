package org.nashtech.com.service;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.nashtech.com.config.PipelineConfig;
import org.nashtech.com.util.AESUtil;
import org.nashtech.com.util.RSAUtil;
import org.nashtech.com.transform.EncryptData;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.util.Map;

public class EncryptionService {
    private final PipelineConfig config;
    private final SecretKey aesKey;
    private final KeyPair rsaKeyPair;

    public EncryptionService(PipelineConfig config) throws Exception {
        this.config = config;
        this.aesKey = AESUtil.generateAESKey();
        this.rsaKeyPair = RSAUtil.generateRSAKeyPair();
    }

    public PCollection<Map<String, String>> encrypt(PCollection<Map<String, String>> input) {
        return input.apply("Encrypt Data", ParDo.of(new EncryptData(
                config.getSchema(),
                config.getPersonalInfoColumns(),
                config.getKmsKeyUri()
        )));
    }

    public String getEncryptedAesKey() {
        return RSAUtil.encryptAESKeyWithRSA(aesKey, rsaKeyPair.getPublic());
    }
}