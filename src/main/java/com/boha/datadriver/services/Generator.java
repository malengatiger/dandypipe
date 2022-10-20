package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.CityPlace;
import com.boha.datadriver.models.Event;
import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.firebase.cloud.FirestoreClient;
//import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.logging.Logger;

@Service
public class Generator {
    static final Logger LOGGER = Logger.getLogger(Generator.class.getSimpleName());

    public Generator(CityService cityService, PlacesService placesService) {
        this.cityService = cityService;
        this.placesService = placesService;
        LOGGER.info(E.LEAF+E.LEAF + " Generator constructed and services injected");
    }


//    private final PubSubAdmin pubSubAdmin;

    private  ArrayList<Subscriber> allSubscribers = new ArrayList<>();
    private final CityService cityService;
    private final PlacesService placesService;

    Random random = new Random(System.currentTimeMillis());
    Timer timer;
    List<City> cityList;
    int totalCount = 0;
    int maxCount;
    public void generateEvents(long intervalInSeconds, int upperCountPerPlace, int maxCount) throws Exception {
        this.maxCount = maxCount;
        if (cityList == null || cityList.isEmpty()) {
            cityList = cityService.getCitiesFromFirestore();
        }
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

    void stopTimer() {
        timer.cancel();
        timer = null;
        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR+E.YELLOW_STAR+E.YELLOW_STAR+
                " Generator Timer stopped; events: " +E.LEAF+" "+ totalCount);
    }
    void performWork(int upperCountPerPlace) throws Exception {
        int index = random.nextInt(cityList.size() - 1);
        City city = cityList.get(index);
        List<CityPlace> places = placesService.getPlacesByCity(city.getId());

        int count = random.nextInt(upperCountPerPlace);
        if (count == 0) count = 5;

        for (int i = 0; i < count; i++) {
            int mIndex = random.nextInt(places.size() - 1);
            CityPlace cityPlace = places.get(mIndex);
            writeData(cityPlace);
            sendToPubSub(cityPlace);
            totalCount++;
        }

        if (totalCount > maxCount) {
            stopTimer();
            totalCount = 0;
        }
    }
    Firestore firestore = FirestoreClient.getFirestore();

    void writeData(CityPlace cityPlace) throws Exception{
        Event event = new Event();
        event.setCityPlace(cityPlace);
        event.setEventId(UUID.randomUUID().toString());
        int rating = random.nextInt(5);
        if (rating == 0) rating = 1;
        event.setRating(rating);
        int m = random.nextInt(200);
        double amt = Double.parseDouble("" + m);
        if (m == 0) amt = 10.00;
        event.setAmount(amt);
        event.setDate(new Date().toString());
        event.setLongDate(new Date().getTime());

        ApiFuture<DocumentReference> future = firestore.collection("events").add(event);
        LOGGER.info(E.LEAF+E.LEAF+ " Event added to Firestore: "
                + E.ORANGE_HEART + cityPlace.name);
    }
    void sendToPubSub(CityPlace cityPlace) {

    }
}
