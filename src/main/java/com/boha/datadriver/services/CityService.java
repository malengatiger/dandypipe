package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.DB;
import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import com.google.firebase.cloud.FirestoreClient;
import com.google.gson.Gson;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Manages the cities needed for the demo. Cities are written to Firestore from a file
 */
@Service
public class CityService {
    private static final Logger LOGGER = Logger.getLogger(CityService.class.getSimpleName());

    @Autowired
    private StorageService storageService;
    @Value("${citiesFile}")
    private String citiesFile;
    public CityService() {
        LOGGER.info(E.AMP+E.AMP+E.AMP + " CityService constructed");
    }
    @Autowired
    ResourceLoader resourceLoader;

    public List<City>  getCitiesFromFile() throws Exception
    {
        LOGGER.info(E.BLUE_DOT+E.BLUE_DOT+ " getCitiesFromFile running ... ");
        try {
            String json = storageService.downloadObject(citiesFile);
            Gson gson = new Gson();
            City[] cities = gson.fromJson(json, City[].class);
            LOGGER.info(E.BLUE_DOT + E.BLUE_DOT + " Found " + cities.length + " cities from json file");
            int ind = 0;
            List<City> realCities = new ArrayList<>(Arrays.asList(cities));
            for (City city : realCities) {
                city.setLatitude(Double.parseDouble(city.getLat()));
                city.setLongitude(Double.parseDouble(city.getLng()));
                city.setId(UUID.randomUUID().toString());
            }

            LOGGER.info(E.BLUE_DOT + E.BLUE_DOT + " Found " + realCities.size()
                    + " real cities from file");
            return realCities;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<City> addCitiesToFirestore() throws Exception{

        List<City> cities = getCitiesFromFile();
        Firestore c = FirestoreClient.getFirestore();

        for (City realCity : cities) {
            LOGGER.info(E.RED_APPLE + " " + realCity.getCity()
                    + " to Firestore " + E.RED_APPLE);
            ApiFuture<DocumentReference> future = c.collection(DB.cities).add(realCity);
            try {
                LOGGER.info(E.GREEN_APPLE+" Firestore city; path: " + future.get().getPath());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return cities;
    }

    public List<FlatEvent> getEventsFromFirestore(int minutesAgo) throws Exception {
        LOGGER.info(E.PEAR+ " Getting events of the past minutesAgo: " + minutesAgo);
        Firestore c = FirestoreClient.getFirestore();
        long date = DateTime.now().toDateTimeISO().minusMinutes(minutesAgo).getMillis();
        ApiFuture<QuerySnapshot> future = c.collection(DB.events)
                .whereGreaterThan("longDate", date)
                .get();
        QuerySnapshot snapshot = future.get();
        List<QueryDocumentSnapshot> docs = snapshot.getDocuments();
        List<FlatEvent> flatEvents = new ArrayList<>();
        for (QueryDocumentSnapshot doc : docs) {
           FlatEvent city = doc.toObject(FlatEvent.class);
           flatEvents.add(city);
        }

        LOGGER.info(E.AMP + E.AMP + " Found " + flatEvents.size()  + " events from Firestore");
        return flatEvents;
    }
    public City getCityById(String cityId) throws Exception {
        Firestore firestore = FirestoreClient.getFirestore();
        QuerySnapshot snapshot = firestore.collection(DB.cities)
                .whereEqualTo("id", cityId)
                .get().get();
        List<QueryDocumentSnapshot> docs = snapshot.getDocuments();
        City city = null;
        for (QueryDocumentSnapshot doc : docs) {
            city = doc.toObject(City.class);
        }
        if (city == null) {
            LOGGER.info(E.RED_DOT+E.RED_DOT+E.RED_DOT+
                    " City not found! : " + cityId);
            throw new Exception("City not found: " + cityId);
        }
        return city;
    }

    public List<FlatEvent> getCityEvents(String cityId, int minutes) throws Exception {
        LOGGER.info(E.PEAR+ " Getting events of the past minutes: " + minutes);
        Firestore c = FirestoreClient.getFirestore();
        long date = DateTime.now().toDateTimeISO().minusMinutes(minutes).getMillis();
        ApiFuture<QuerySnapshot> future = c.collection(DB.events)
                .whereEqualTo("cityId", cityId)
                .whereGreaterThan("longDate", date)
                .get();
        QuerySnapshot snapshot = future.get();
        List<QueryDocumentSnapshot> docs = snapshot.getDocuments();
        List<FlatEvent> flatEvents = new ArrayList<>();
        for (QueryDocumentSnapshot doc : docs) {
            FlatEvent city = doc.toObject(FlatEvent.class);
            flatEvents.add(city);
        }

        LOGGER.info(E.AMP + E.AMP + " Found " + flatEvents.size()  + " events from Firestore");
        return flatEvents;
    }

    public long countCities() throws Exception {
        Firestore firestore = FirestoreClient.getFirestore();
        AggregateQuerySnapshot snapshot = firestore.collection(DB.cities)
                .count()
                .get().get();
        long count = snapshot.getCount();
        LOGGER.info(E.AMP + " Counted " + count + " cities");
        return count;
    }


    public List<City> getCities() throws Exception {
        LOGGER.info(E.PEAR+ " Getting all cities ...");
        Firestore c = FirestoreClient.getFirestore();
        ApiFuture<QuerySnapshot> future = c.collection(DB.cities)
                .orderBy("city")
                .get();
        QuerySnapshot snapshot = future.get();
        List<QueryDocumentSnapshot> docs = snapshot.getDocuments();
        List<City> cities = new ArrayList<>();
        for (QueryDocumentSnapshot doc : docs) {
            City city = doc.toObject(City.class);
            cities.add(city);
        }

        LOGGER.info(E.CHECK + E.CHECK + " Found " + cities.size()  + " cities from Firestore");
        return cities;
    }


}
