package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.google.gson.Gson;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Manages the cities needed for the demo. Cities are written to Firestore from a file
 */
@Service
public class EventService {
    private static final Logger LOGGER = Logger.getLogger(EventService.class.getSimpleName());

    @Autowired
    private StorageService storageService;
    @Value("${citiesFile}")
    private String citiesFile;
    public EventService() {
        LOGGER.info(E.AMP+E.AMP+E.AMP + " EventService constructed");
    }

    public List<FlatEvent> getRecentEvents(int  hours) throws Exception{
        List<FlatEvent> events  = new ArrayList<>();
        Firestore c = FirestoreClient.getFirestore();
        long date = DateTime.now().minusHours(hours).getMillis();
        ApiFuture<QuerySnapshot>  future = c.collection("events")
                .whereGreaterThan("longDate", date).
                orderBy("date").get();
        for (QueryDocumentSnapshot document : future.get().getDocuments()) {
            FlatEvent e = document.toObject(FlatEvent.class);
            events.add(e);
        }

        return events;
    }

}
