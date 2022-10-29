package com.boha.datadriver.util;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

@Service
public class SecretMgr {
    static final Logger LOGGER = Logger.getLogger(SecretMgr.class.getSimpleName());

    private String projectId;
    @Value("${placesAPIKeyName}")
    private String placesAPIKeyName;
    @Value("${secretsVersion}")
    private String secretsVersion;
    @Autowired
    private Environment environment;
    void setProjectId() {
        projectId = environment.getProperty("PROJECT_ID");
    }


    // Access the payload for the given secret version if one exists. The version
    // can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    public String getPlacesAPIKey()
            throws Exception {
        setProjectId();
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests. After completing all of your requests, call
        // the "close" method on the client to safely clean up any remaining background resources.
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            SecretVersionName secretVersionName = SecretVersionName.of(projectId, placesAPIKeyName, secretsVersion);

            // Access the secret version.
            AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);

            // Verify checksum. The used library is available in Java 9+.
            // If using Java 8, you may use the following:
            // https://github.com/google/guava/blob/e62d6a0456420d295089a9c319b7593a3eae4a83/guava/src/com/google/common/hash/Hashing.java#L395
            byte[] data = response.getPayload().getData().toByteArray();
            Checksum checksum = new CRC32C();
            checksum.update(data, 0, data.length);
            if (response.getPayload().getDataCrc32C() != checksum.getValue()) {
                LOGGER.info("Data corruption detected.");
                throw new Exception("Data corruption detected");
            }

            // Print the secret payload.
            //
            // WARNING: Do not print the secret in a production environment - this
            // snippet is showing how to access the secret material.
            String payload = response.getPayload().getData().toStringUtf8();
            String check = payload.substring(0,4);
            if (check.contains("AIza")) {
                LOGGER.info(E.RED_DOT + E.RED_DOT + E.RED_DOT +
                        " Secret Payload contains: " + check);
            }
            return payload;
        }
    }

}
