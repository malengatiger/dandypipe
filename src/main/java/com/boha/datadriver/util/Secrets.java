package com.boha.datadriver.util;

import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.Replication;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;
@Service
public class Secrets {
    static final Logger LOGGER = Logger.getLogger(Secrets.class.getSimpleName());
    @Value("${projectId}")
    private String projectId;
    @Value("${secretId}")
    private String secretId;

    public String getSecret() throws Exception {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests. After completing all of your requests, call
        // the "close" method on the client to safely clean up any remaining background resources.
        String apiKey = null;
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            ProjectName projectName = ProjectName.of(projectId);
            // Create the parent secret.
            Secret secret =
                    Secret.newBuilder()
                            .setReplication(
                                    Replication.newBuilder()
                                            .setAutomatic(Replication.Automatic.newBuilder().build())
                                            .build())
                            .build();

            Secret mySecret = client.getSecret(secretId);

            //Secret createdSecret = client.createSecret(projectName, secretId, secret);
            LOGGER.info(E.RED_APPLE+ "Secret; name: " + mySecret.toString() + " hasExpiration: " + mySecret.hasExpireTime());
            apiKey = mySecret.getName();
        } catch ( Exception e) {
            throw e;
        }
        return apiKey;
    }
}
