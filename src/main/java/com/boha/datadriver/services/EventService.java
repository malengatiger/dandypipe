package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.DB;
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
    private static final String collectionName = DB.events;

    @Autowired
    private StorageService storageService;
    @Autowired
    private CityService cityService;
    @Value("${citiesFile}")
    private String citiesFile;
    public EventService() {
        LOGGER.info(E.AMP+E.AMP+E.AMP + " EventService constructed");
    }

    public List<FlatEvent> getRecentEvents(int  minutes) throws Exception{
        List<FlatEvent> events  = new ArrayList<>();
        Firestore c = FirestoreClient.getFirestore();
        long deltaDate = DateTime.now().minusMinutes(minutes).getMillis();
        ApiFuture<QuerySnapshot>  future = c.collection(DB.events)
                .whereGreaterThan("longDate", deltaDate).
                orderBy("longDate").get();
        for (QueryDocumentSnapshot document : future.get().getDocuments()) {
            FlatEvent e = document.toObject(FlatEvent.class);
            if (e.getAmount() > 0.0) {
                events.add(e);
            }
        }
        LOGGER.info("\uD83D\uDC99\uD83D\uDC99\uD83D\uDC99 Recent events found: " + events.size());
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
    public long countEvents(int minutes) throws Exception {
        Firestore c = FirestoreClient.getFirestore();
        long date = DateTime.now().minusMinutes(minutes).getMillis();
        AggregateQuery q  = c.collection(collectionName).whereGreaterThan("longDate",date).count();
        long count = q.get().get().getCount();

        LOGGER.info(E.RED_APPLE + " " + count +" Events found in the last " + minutes + " minutes");
        return count;
    }

    public List<FlatEvent> getCityEvents(String cityId, int minutesAgo) throws Exception {
//        LOGGER.info(E.PEAR+ " Control paging ------------------------- collect city events using pagination");
        City city = cityService.getCityById(cityId);
        MyBag myBag = usePagination(cityId, minutesAgo, null);
        List<FlatEvent> events = new ArrayList<>(myBag.events);
        LOGGER.info("\n" +E.PEAR+E.PEAR+E.PEAR+" Paged events: " + city.getCity());
        LOGGER.info(E.PEAR+ "Firestore paging - first page delivered: " + myBag.getEvents().size() + " " +
                E.RED_APPLE);

        var control = 0;
        var cnt = 0;
        while (control == 0) {
            myBag = usePagination(cityId, minutesAgo, myBag.documentSnapshot);
            if (myBag.events.isEmpty()) {
                control = 1;
            } else {
                events.addAll(myBag.events);
                cnt++;
                LOGGER.info(E.PEAR + " Events added to list: "+ E.RED_APPLE + myBag.getEvents().size() + ", " +
                        "total events: " + events.size() + E.RED_APPLE + " page #" + cnt);
            }

        }

//        LOGGER.info(E.PEAR+E.PEAR+E.PEAR+E.PEAR
//                +" Firestore total results,  "+E.CHECK + " "
//                + events.size() + " events; pages required: " + (cnt+1) + " " + E.ORANGE_HEART);
        return events;
    }


    private MyBag usePagination(String cityId, int minutesAgo, DocumentSnapshot documentSnapshot) throws Exception {

        Firestore c = FirestoreClient.getFirestore();
        DateTime dt = DateTime.now().toDateTimeISO().minusMinutes(minutesAgo);
        List<FlatEvent> list = new ArrayList<>();

        QuerySnapshot snapshot;
        if (documentSnapshot == null) {
            snapshot = c.collection(DB.events)
                    .whereEqualTo("cityId", cityId)
                    .whereGreaterThanOrEqualTo("longDate", dt.getMillis())
                    .orderBy("longDate", Query.Direction.ASCENDING)
                    .limit(10000)
                    .get().get();
        } else {
            snapshot = c.collection(DB.events)
                    .whereEqualTo("cityId", cityId)
                    .whereGreaterThanOrEqualTo("longDate", dt.getMillis())
                    .orderBy("longDate", Query.Direction.ASCENDING)
                    .startAfter(documentSnapshot)
                    .limit(10000)
                    .get().get();
        }
//        LOGGER.info(E.PEAR+ " Control paging usePagination - query executed ... " + E.RED_DOT+E.RED_APPLE);

        DocumentReference reference = null;
        for (QueryDocumentSnapshot doc : snapshot.getDocuments()) {
            FlatEvent data = doc.toObject(FlatEvent.class);
            list.add(data);
            reference = doc.getReference();
        }
//        LOGGER.info(E.PEAR+ " Control paging usePagination - "+E.BLUE_HEART+" snapshot documents in my list: "
//                + list.size());
        MyBag bag;
        if (reference != null) {
            DocumentSnapshot docSnapshot = reference.get().get();
            bag = new MyBag(docSnapshot, list);
        } else {
            bag = new MyBag(null, new ArrayList<>());
//            LOGGER.info(E.PEAR +E.PEAR +E.PEAR +E.PEAR + " Control paging - WE ARE DONE! "+ E.PEAR);

        }

        return bag;
    }

}

class MyBag {
    DocumentSnapshot documentSnapshot;
    List<FlatEvent> events;

    public MyBag(DocumentSnapshot documentSnapshot, List<FlatEvent> dashboards) {
        this.documentSnapshot = documentSnapshot;
        this.events = dashboards;
    }

    public DocumentSnapshot getDocumentSnapshot() {
        return documentSnapshot;
    }

    public void setDocumentSnapshot(DocumentSnapshot documentSnapshot) {
        this.documentSnapshot = documentSnapshot;
    }

    public List<FlatEvent> getEvents() {
        return events;
    }

    public void setEvents(List<FlatEvent> events) {
        this.events = events;
    }
}