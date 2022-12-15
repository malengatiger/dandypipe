package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.CityAggregate;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.DB;
import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firebase.cloud.FirestoreClient;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.logging.Logger;

@Service
public class CityAggregateService {
    private static final Logger LOGGER = Logger.getLogger(CityAggregateService.class.getSimpleName());

    public CityAggregateService(CityService cityService) {
        this.cityService = cityService;
        LOGGER.info(E.RED_DOT +" CityCountService constructed and CityService injected");
    }

    final CityService cityService;

    public CityAggregate createCityAggregate(String cityId, int minutesAgo) throws Exception {
        long start = System.currentTimeMillis();
        List<FlatEvent> eventList = cityService.getCityEvents(cityId,minutesAgo);
        long end1 = System.currentTimeMillis();
        double elapsed = Double.parseDouble(String.valueOf((end1-start)/1000));
        LOGGER.info(E.RED_APPLE + " getCityEvents took " + elapsed
                + " seconds to get events from Firestore, minutesAgo: " + minutesAgo + " " + E.RED_APPLE);

        int numberOfEvents = eventList.size();
        int totalAmount = 0;
        double averageRating = 0.0;
        int totalRating = 0;

        for (FlatEvent flatEvent : eventList) {
            totalAmount += flatEvent.getAmount();
            totalRating += flatEvent.getRating();
        }
        averageRating = Double.parseDouble("" + totalRating) /
                Double.parseDouble("" + numberOfEvents);
        long end2 = System.currentTimeMillis();
        double elapsed2 = Double.parseDouble(String.valueOf((end2-start)/1000));
        City city = cityService.getCityById(cityId);
        CityAggregate ca = new CityAggregate();
        ca.setCityId(cityId);
        ca.setCityName(city.getCity());
        ca.setDate(DateTime.now().toDateTimeISO().toString());
        ca.setLongDate(DateTime.now().toDateTimeISO().getMillis());
        ca.setNumberOfEvents(numberOfEvents);
        ca.setAverageRating(averageRating);
        ca.setTotalSpent(totalAmount);
        ca.setMinutesAgo(minutesAgo);
        ca.setElapsedSeconds(elapsed2);
        ca.setLatitude(city.getLatitude());
        ca.setLongitude(city.getLongitude());

        LOGGER.info(E.RED_APPLE + " aggregate: "
                + " - totalSpent: " + ca.getTotalSpent()
                + " ratingAverage: " + ca.getAverageRating()
                + " for " + E.LEAF + city.getCity());
        LOGGER.info(E.RED_APPLE + " adding " + city.getCity()
                + " aggregate to Firestore " + E.RED_APPLE);
        Firestore c = FirestoreClient.getFirestore();
        String id = c.collection(DB.cityAggregates).add(ca).get().getId();
        LOGGER.info(E.RED_APPLE + " added aggregate, id = " + id
                + " to Firestore " + E.RED_APPLE);
        long end3 = System.currentTimeMillis();
        elapsed = Double.parseDouble(String.valueOf((end3-start)/1000));
        LOGGER.info(E.RED_APPLE + " createCityAggregate took " + elapsed
                + " seconds to wrap up aggregate! " + E.RED_APPLE);

        return ca;

    }

    public List<CityAggregate> getCityAggregatesByCity(String cityId, int minutesAgo) throws Exception {
        LOGGER.info(E.RED_APPLE + " Get City Aggregates, minutesAgo" + minutesAgo);
        Firestore c = FirestoreClient.getFirestore();
        List<CityAggregate> list = new ArrayList<>();
        DateTime dt = DateTime.now().minusMinutes(minutesAgo);
        QuerySnapshot id = c.collection(DB.cityAggregates)
                .whereEqualTo("cityId",cityId)
                .whereGreaterThanOrEqualTo("longDate", dt.getMillis())
                .get().get();
        for (QueryDocumentSnapshot doc : id.getDocuments()) {
            list.add(doc.toObject(CityAggregate.class));
        }
        LOGGER.info(E.RED_APPLE + " retrieved aggregates = " + list.size()
                + " from Firestore, minutesAgo: " + minutesAgo + E.RED_APPLE);
        return null;
    }

    public List<CityAggregate> getCityAggregates(int minutesAgo) throws Exception {
        LOGGER.info(E.RED_APPLE + " Get City Aggregates, minutesAgo: " + minutesAgo);
        Firestore c = FirestoreClient.getFirestore();
        List<CityAggregate> list = new ArrayList<>();
        DateTime dt = DateTime.now().minusMinutes(minutesAgo);
        QuerySnapshot id = c.collection(DB.cityAggregates)
                .whereGreaterThanOrEqualTo("longDate", dt.getMillis())
                .get().get();
        for (QueryDocumentSnapshot doc : id.getDocuments()) {
            list.add(doc.toObject(CityAggregate.class));
        }
        LOGGER.info(E.RED_APPLE + " retrieved aggregates = " + list.size()
                + " from Firestore, minutesAgo: " + minutesAgo + E.RED_APPLE);
        return null;
    }

    public List<CityAggregate> createAggregatesForAllCities(int minutesAgo) throws Exception {
        LOGGER.info(E.AMP+E.AMP+E.AMP+" .... createAggregatesForAllCities starting ....");
        long start = System.currentTimeMillis();
        final List<City> cities = cityService.getCitiesFromFirestore();
        List<CityAggregate> aggregates = new ArrayList<>();
        for (City city : cities) {
            CityAggregate ca = createCityAggregate(city.getId(), minutesAgo);
            aggregates.add(ca);
        }

        long end = System.currentTimeMillis();
        double elapsed = Double.parseDouble(String.valueOf((end-start)/1000));
        LOGGER.info(E.AMP+E.AMP+E.AMP+E.RED_APPLE + " createAggregatesForAllCities took " + elapsed
                + " seconds to calculate aggregates from Firestore " + E.RED_APPLE);
        return aggregates;
    }

}
