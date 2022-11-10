package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.*;
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
    private static final String collectionName = "flatEvents";

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
        long deltaDate = DateTime.now().minusHours(hours).getMillis();
        ApiFuture<QuerySnapshot>  future = c.collection(collectionName)
                .whereGreaterThan("longDate", deltaDate).
                orderBy("longDate").get();
        for (QueryDocumentSnapshot document : future.get().getDocuments()) {
            FlatEvent e = document.toObject(FlatEvent.class);
            events.add(e);
        }

        return events;
    }
    public FlatEvent getLastEvent(int hours) throws Exception{
        List<FlatEvent> events = getRecentEvents(hours);
        if (events.size() > 0) {
            FlatEvent event = events.get(events.size() - 1);
            LOGGER.info(E.RED_APPLE + " Last Event withing " + hours + " hours: "
                    + event.getDate() + " " + event.getCityName() + " " + event.getPlaceName());
            return event;
        }
        return null;
    }
    public long countEvents(int hours) throws Exception {
        Firestore c = FirestoreClient.getFirestore();
        long date = DateTime.now().minusHours(hours).getMillis();
        AggregateQuery q  = c.collection(collectionName).whereGreaterThan("longDate",date).count();
        long count = q.get().get().getCount();

        LOGGER.info(E.RED_APPLE + " " + count +" Events found in the last " + hours + " hours");
        return count;
    }


}
