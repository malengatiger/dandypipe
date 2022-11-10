package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.CityPlace;
import com.boha.datadriver.models.Event;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.E;
import com.boha.datadriver.util.FlatEventGetter;
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
//@EnableScheduling
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


    public CityPlace generateCrowd(String cityId, int total) throws Exception {

        List<CityPlace> places;
        try {
            places = placesService.getPlacesByCity(cityId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        int index = random.nextInt(places.size() - 1);
        CityPlace place = places.get(index);
        if (place.name.equalsIgnoreCase(place.cityName)) {
            LOGGER.info(E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR
                    +" Ignore this place for crowd generation: " + place.name);
            generateCrowd(cityId,total);
        }
        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR+E.RED_APPLE +
                " Generating crowd: " + place.name + ", " + place.cityName);
        int x = 0;
        for (int  i = 0; i < total; i++) {
            x += generateEventAtPlace(place);
        }

        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR
                + " Done generating a crowd of " + x + " at: " + place.name +", "+place.cityName + E.RED_APPLE);

        return place;
    }

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
        if (count < 10) count = 20;

        int x = 0;
        for (int i = 0; i < count; i++) {
            int mIndex = random.nextInt(places.size() - 1);
            CityPlace cityPlace = places.get(mIndex);
            x += generateEventAtPlace(cityPlace);
        }

        LOGGER.info(E.LEAF+E.LEAF + " Total Events generated: count: " + x +
                E.RED_APPLE + " totalCount: " + totalCount + " at " + DateTime.now().toDateTimeISO().toString());
        if (totalCount > maxCount) {
            stopTimer();
            totalCount = 0;
        }
    }

    private int generateEventAtPlace(CityPlace cityPlace) {
        Event event;
        try {
            event = getEvent(cityPlace);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!event.getCityPlace().cityName.equalsIgnoreCase(event.getCityPlace().name)) {
            event.setDate(DateTime.now().toDateTimeISO().toString());
            event.setLongDate(DateTime.now().getMillis());
            try {
                writeEventToFirestore(event);
                sendToPubSub(event);
            } catch (Exception e) {
                LOGGER.info(E.RED_DOT+E.RED_DOT+E.RED_DOT+ " Problemo Senor? " + e.getMessage());
                e.printStackTrace();
            }
            totalCount++;
            return 1;
        } else {
            LOGGER.info(E.RED_DOT+E.RED_DOT+E.RED_DOT+
                    " Event ignored, city and place name are the same" +
                    E.AMP+E.AMP);
            return 0;
        }
    }

    private  Event getEvent(CityPlace cityPlace)  {
        Event event = new Event();
        event.setCityPlace(cityPlace);
        int m = random.nextInt(2500);
        if (m == 0) m = 150;
        int cents = random.nextInt(99);
        if (cents  < 10) cents = 50;
        event.setAmount(Double.parseDouble("" + m + "." + cents));
        int r = random.nextInt(5);
        if (r == 0) r = 5;
        event.setRating(r);
        event.setDate(DateTime.now().toDateTimeISO().toString());
        event.setLongDate(DateTime.now().getMillis());
        event.setEventId(UUID.randomUUID().toString());
        return event;
    }

     Firestore firestore;

    private void writeEventToFirestore(Event event) throws Exception {
        if (firestore == null) {
            firestore = FirestoreClient.getFirestore();
        }
        FlatEvent flatEvent = FlatEventGetter.getFlatEvent(event);
        ApiFuture<DocumentReference> future =
                firestore.collection("flatEvents").add(flatEvent);
        LOGGER.info(E.LEAF + E.LEAF + " Firestore: "
                + E.ORANGE_HEART + flatEvent.getPlaceName() + ", " + flatEvent.getCityName()
                + " " + E.LEAF);
    }

    private void sendToPubSub(Event event) throws Exception {

        eventPublisher.publishEvent(GSON.toJson(event));
        FlatEvent fe = FlatEventGetter.getFlatEvent(event);

        eventPublisher.publishFlatEvent(GSON.toJson(fe));
        eventPublisher.publishBigQueryEvent(GSON.toJson(fe));
        eventPublisher.publishPull(GSON.toJson(fe));

        LOGGER.info(E.BLUE_HEART + E.BLUE_HEART +
                " PubSub Event: " + E.AMP + fe.getPlaceName() + ", " + fe.getCityName());
    }
}
