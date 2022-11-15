package com.boha.datadriver.services;

import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Initializes Firebase
 */
@Service
public class FirebaseService {
    private static final Logger LOGGER = Logger.getLogger(FirebaseService.class.getSimpleName());

    public FirebaseService() {
        LOGGER.info(E.AMP+E.AMP+E.AMP + " FirebaseService constructed");
    }
//    @Autowired
//    private Environment environment;
    private FirebaseApp app;
    public FirebaseApp initializeFirebase() {
        LOGGER.info(E.AMP+E.AMP+E.AMP+ " .... initializing Firebase ....");
        FirebaseOptions options;
        String projectId = System.getenv().get("PROJECT_ID");
        if (projectId == null) {
            LOGGER.info(E.RED_DOT+E.RED_DOT+E.AMP+ " .... missing ProjectId WTF? ....");
            throw  new RuntimeException("Project  ID is missing from environment variables");
        }
        LOGGER.info(E.AMP+E.AMP+E.AMP+
                " Project Id from System.getenv: "+E.RED_APPLE + " " + projectId);
        try {
            options = FirebaseOptions.builder()
                    .setCredentials(GoogleCredentials.getApplicationDefault())
                    .setDatabaseUrl("https://" + projectId + ".firebaseio.com/")
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Firebase initialization failed!  " + e.getMessage());
        }

        app = FirebaseApp.initializeApp(options);
        LOGGER.info(E.AMP+E.AMP+E.AMP+E.AMP+E.AMP+
                " Firebase has been initialized: "
                + app.getOptions().getDatabaseUrl()
                + " " + E.RED_APPLE);
        return app;
    }
}
