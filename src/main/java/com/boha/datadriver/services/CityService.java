package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.CityAggregate;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
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
    public List<City>  getCitiesFromFile() throws Exception{
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
            ApiFuture<DocumentReference> future = c.collection("cities").add(realCity);
            try {
                LOGGER.info(E.GREEN_APPLE+" Firestore city; path: " + future.get().getPath());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return cities;
    }
    public List<CityAggregate> getCityAggregates(int hours) throws Exception {

        List<FlatEvent> events = getEventsFromFirestore(hours);
        LOGGER.info(E.RED_APPLE
                +" Events of last " + hours + " hours found: " + events.size());
        HashMap<String, List<FlatEvent>> eMap = new HashMap<>();
        for (FlatEvent event : events) {
            if (eMap.containsKey(event.getCityId())) {
                List<FlatEvent> list = eMap.get(event.getCityId());
                list.add(event);
            } else {
                List<FlatEvent> list = new ArrayList<>();
                list.add(event);
                eMap.put(event.getCityId(),list);
            }
        }
        List<CityAggregate> aggList = new ArrayList<>();
        Set<String> set = eMap.keySet();
        for (String cityId : set) {
            List<FlatEvent> eventList = eMap.get(cityId);
            int numberOfEvents = eventList.size();
            int totalAmount = 0;
            double averageRating = 0.0;
            int totalRating = 0;

            for (FlatEvent flatEvent : eventList) {
                totalAmount += flatEvent.getAmount();
                totalRating += flatEvent.getRating();
            }
            averageRating = Double.parseDouble(""+totalRating) /
                    Double.parseDouble(""+numberOfEvents);
            City city = getCityById(cityId);
            CityAggregate ca = new CityAggregate();
            ca.setCityId(cityId);
            ca.setCityName(city.getCity());
            ca.setDate(DateTime.now().toDateTimeISO().toString());
            ca.setLongDate(DateTime.now().toDateTimeISO().getMillis());
            ca.setNumberOfEvents(numberOfEvents);
            ca.setAverageRating(averageRating);
            ca.setTotalSpent(totalAmount);
            ca.setHours(hours);
            aggList.add(ca);
            LOGGER.info(E.RED_APPLE+ " aggregate: "
                    + " - totalSpent: " + ca.getTotalSpent()
            + " ratingAverage: " + ca.getAverageRating()
            + " for " + E.LEAF + city.getCity() );

        }
        LOGGER.info(E.RED_APPLE+
                " City Aggregates generated: " + aggList.size());
        Collections.sort(aggList);
        return aggList;
    }
    public List<FlatEvent> getEventsFromFirestore(int hours) throws Exception {
        LOGGER.info(E.PEAR+ " Getting events of the past hours: " + hours);
        Firestore c = FirestoreClient.getFirestore();
        long date = DateTime.now().toDateTimeISO().minusHours(hours).getMillis();
        ApiFuture<QuerySnapshot> future = c.collection("flatEvents")
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
        ApiFuture<QuerySnapshot> future = firestore.collection("cities")
                .whereEqualTo("id", cityId)
                .get();
        QuerySnapshot snapshot = future.get();
        List<QueryDocumentSnapshot> docs = snapshot.getDocuments();
        City city = null;
        for (QueryDocumentSnapshot doc : docs) {
            city = doc.toObject(City.class);
        }
        if (city == null) {
            LOGGER.info(E.RED_DOT+E.RED_DOT+E.RED_DOT+
                    " City not found! : " + cityId);
        }
        return city;
    }

    public List<City> getCitiesFromFirestore() throws Exception {
        LOGGER.info(E.PEAR+ " Getting cities ...");
        Firestore c = FirestoreClient.getFirestore();
        ApiFuture<QuerySnapshot> future = c.collection("cities")
                .orderBy("city")
                .get();
        QuerySnapshot snapshot = future.get();
        List<QueryDocumentSnapshot> docs = snapshot.getDocuments();
        List<City> flatEvents = new ArrayList<>();
        for (QueryDocumentSnapshot doc : docs) {
            City city = doc.toObject(City.class);
            flatEvents.add(city);
        }

        LOGGER.info(E.CHECK + E.CHECK + " Found " + flatEvents.size()  + " cities from Firestore");
        return flatEvents;
    }


}
