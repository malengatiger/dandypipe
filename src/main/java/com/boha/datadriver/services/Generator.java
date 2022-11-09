package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.CityPlace;
import com.boha.datadriver.models.Event;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.firebase.cloud.FirestoreClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.logging.Logger;

/**
 * Manages the creation of streaming data to PubSub and Firestore. Events are generated and sent forth
 */
@Service
public class Generator {
    static final Logger LOGGER = Logger.getLogger(Generator.class.getSimpleName());
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();


    public Generator(CityService cityService, PlacesService placesService, EventPublisher eventPublisher) {
        this.cityService = cityService;
        this.placesService = placesService;
        this.eventPublisher = eventPublisher;
    }


//    private final PubSubAdmin pubSubAdmin;

    private final ArrayList<Subscriber> allSubscribers = new ArrayList<>();
    private  final CityService cityService;
    private final PlacesService placesService;
    private final EventPublisher eventPublisher;

    @Autowired
    private EventSubscriber eventSubscriber;

    private static Random random = new Random(System.currentTimeMillis());
    static Timer timer;
    static List<City> cityList;
    static int totalCount = 0;
    static int maxCount;

    public void generateEvents(long intervalInSeconds, int upperCountPerPlace, int maximumCount) throws Exception {
        maxCount = maximumCount;
        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT +E.BLUE_DOT +E.BLUE_DOT +E.BLUE_DOT +
                " Generator: generateEvents\n intervalInSeconds: " + intervalInSeconds
                + " upperCountPerPlace: " +
                +upperCountPerPlace + " maxCount: " + maxCount +  " " + E.BLUE_DOT+E.BLUE_DOT);
        if (cityList == null || cityList.isEmpty()) {
            cityList = cityService.getCitiesFromFirestore();
        }
        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT + " starting Timer to control work ...");
//        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
//        executorService.scheduleAtFixedRate(Runner::performWork, 0, 5, TimeUnit.SECONDS);
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    performWork(upperCountPerPlace);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, 1000, intervalInSeconds * 1000);

    }
    public void stopTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;
            LOGGER.info(E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR +
                    " Generator Timer stopped; events: " + E.LEAF + " totalCount: " + totalCount);
        }
    }

    private void performWork(int upperCountPerPlace)  {
        int index = random.nextInt(cityList.size() - 1);
        City city = cityList.get(index);
        List<CityPlace> places = null;
        try {
            places = placesService.getPlacesByCity(city.getId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int count = random.nextInt(upperCountPerPlace);
        if (count == 0) count = 5;

        for (int i = 0; i < count; i++) {
            int mIndex = random.nextInt(places.size() - 1);
            CityPlace cityPlace = places.get(mIndex);
            Event event = null;
            try {
                event = getEvent(cityPlace);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            if (!event.getCityPlace().cityName.equalsIgnoreCase(event.getCityPlace().name)) {
                event.setDate(DateTime.now().toDateTimeISO().toString());
                event.setLongDate(DateTime.now().getMillis());
                try {
                    writeEventToFirestore(event, city);
                    sendToPubSub(event, city);
                } catch (Exception e) {
                    LOGGER.info(E.RED_DOT+E.RED_DOT+E.RED_DOT+ " Problemo Senor? " + e.getMessage());
                    e.printStackTrace();
                }
                totalCount++;
            } else {
                LOGGER.info(E.RED_DOT+E.RED_DOT+E.RED_DOT+
                        " Event ignored, city and place name are the same" +
                        E.AMP+E.AMP);
            }
        }

        LOGGER.info(E.LEAF+E.LEAF + " Total Events generated: " + totalCount + " at " + DateTime.now().toDateTimeISO().toString());
        if (totalCount > maxCount) {
            stopTimer();
            totalCount = 0;
        }
    }
    private  Event getEvent(CityPlace cityPlace)  {
        Event event = new Event();
        event.setCityPlace(cityPlace);
        int m = random.nextInt(2500);
        if (m == 0) m = 150;
        event.setAmount(Double.parseDouble("" + m));
        int r = random.nextInt(5);
        if (r == 0) r = 5;
        event.setRating(r);
        event.setDate(DateTime.now().toDateTimeISO().toString());
        event.setLongDate(DateTime.now().getMillis());
        event.setEventId(UUID.randomUUID().toString());
        return event;
    }

     Firestore firestore;

    private void writeEventToFirestore(Event event, City city) throws Exception {
        if (firestore == null) {
            firestore = FirestoreClient.getFirestore();
        }
        ApiFuture<DocumentReference> future =
                firestore.collection("events").add(event);
        LOGGER.info(E.LEAF + E.LEAF + " Firestore event: "
                + E.ORANGE_HEART + event.getCityPlace().name + ", " + city.getCity()
                + " path: " + future.get().getPath() + E.LEAF);
    }



    private void sendToPubSub(Event event, City city) throws Exception {

        LOGGER.info("... publish Event ....");
        eventPublisher.publishEvent(GSON.toJson(event));
        FlatEvent fe = event.getFlatEvent();

        LOGGER.info("... publishFlatEvent ....");
        eventPublisher.publishFlatEvent(GSON.toJson(fe));
        LOGGER.info("... publishBigQueryEvent ....");
        eventPublisher.publishBigQueryEvent(GSON.toJson(fe));
        LOGGER.info("... publishPull ....");
        eventPublisher.publishPull(GSON.toJson(fe));
        LOGGER.info(E.BLUE_HEART + E.BLUE_HEART +
                " PubSub Event: " + E.AMP + event.getCityPlace().name + ", " + city.getCity());
    }
}
