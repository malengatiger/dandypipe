package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.CityAggregate;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.DB;
import com.boha.datadriver.util.E;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
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
                + " seconds to get "+eventList.size()+" events from Firestore, minutesAgo: " + minutesAgo + " " + E.RED_APPLE);

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

        City cityById = cityService.getCityById(cityId);

        long end2 = System.currentTimeMillis();
        double elapsed2 = Double.parseDouble("" + (end2-start)/1000);

        CityAggregate cityAggregate = new CityAggregate();
        cityAggregate.setCityId(cityById.getId());
        cityAggregate.setCityName(cityById.getCity());
        cityAggregate.setDate(DateTime.now().toDateTimeISO().toString());
        cityAggregate.setLongDate(DateTime.now().toDateTimeISO().getMillis());
        cityAggregate.setNumberOfEvents(numberOfEvents);
        cityAggregate.setAverageRating(averageRating);
        cityAggregate.setTotalSpent(totalAmount);
        cityAggregate.setMinutesAgo(minutesAgo);
        cityAggregate.setElapsedSeconds(elapsed2);
        cityAggregate.setLatitude(cityById.getLatitude());
        cityAggregate.setLongitude(cityById.getLongitude());

        LOGGER.info(E.RED_APPLE + " aggregate: "
                + " - totalSpent: " + cityAggregate.getTotalSpent()
                + " ratingAverage: " + cityAggregate.getAverageRating()
                + " elapsedSeconds: " + cityAggregate.getElapsedSeconds()
                + " for " + E.LEAF + cityById.getCity());
        LOGGER.info(E.RED_APPLE + " adding " + cityById.getCity()
                + " aggregate to Firestore " + E.RED_APPLE);
        //add aggregate to Firestore
        Firestore c = FirestoreClient.getFirestore();
        String id = c.collection(DB.cityAggregates).add(cityAggregate).get().getId();
        LOGGER.info(E.RED_APPLE + " added aggregate, id = " + id
                + " to Firestore " + E.RED_APPLE);
        long end3 = System.currentTimeMillis();
        elapsed = Double.parseDouble(String.valueOf((end3-start)/1000));
        LOGGER.info(E.RED_APPLE + " createCityAggregate took " + elapsed
                + " seconds to wrap up aggregate! " + E.RED_APPLE);

        return cityAggregate;

    }

    public List<CityAggregate> getCityAggregatesByCity(String cityId, int minutesAgo) throws Exception {
        LOGGER.info(E.RED_APPLE + " Get City Aggregates, minutesAgo" + minutesAgo);
        Firestore c = FirestoreClient.getFirestore();
        List<CityAggregate> list = new ArrayList<>();
        DateTime dt = DateTime.now().minusMinutes(minutesAgo);
        QuerySnapshot id = c.collection(DB.cityAggregates)
                .whereEqualTo("cityId",cityId)
                .whereGreaterThanOrEqualTo("longDate", dt.getMillis())
                .orderBy("longDate", Query.Direction.DESCENDING)
                .get().get();
        for (QueryDocumentSnapshot doc : id.getDocuments()) {
            list.add(doc.toObject(CityAggregate.class));
        }
        LOGGER.info(E.RED_APPLE + " retrieved aggregates = " + list.size()
                + " from Firestore, minutesAgo: " + minutesAgo + E.RED_APPLE);
        return list;
    }

    public List<CityAggregate> getCityAggregates(int minutesAgo) throws Exception {
        LOGGER.info(E.RED_APPLE + " Get City Aggregates, minutesAgo: " + minutesAgo);
        Firestore c = FirestoreClient.getFirestore();
        List<CityAggregate> list = new ArrayList<>();
        DateTime dt = DateTime.now().minusMinutes(minutesAgo);
        QuerySnapshot id = c.collection(DB.cityAggregates)
                .whereGreaterThanOrEqualTo("longDate", dt.getMillis())
                .orderBy("longDate", Query.Direction.DESCENDING)
                .get().get();
        for (QueryDocumentSnapshot doc : id.getDocuments()) {
            list.add(doc.toObject(CityAggregate.class));
        }
        LOGGER.info(E.RED_APPLE + " retrieved aggregates = " + list.size()
                + " from Firestore, minutesAgo: " + minutesAgo + E.RED_APPLE);
        return list;
    }

    public List<CityAggregate> createAggregatesForAllCities(int minutesAgo) throws Exception {
        LOGGER.info(E.AMP+E.AMP+E.AMP+" .... createAggregatesForAllCities starting ....");
        long start = System.currentTimeMillis();
        final List<City> cities = cityService.getCities();
        List<CityAggregate> aggregates = new ArrayList<>();

        for (City mCity : cities) {
            CityAggregate ca = createCityAggregate(mCity.getId(), minutesAgo);
            aggregates.add(ca);
        }

        long end = System.currentTimeMillis();
        double elapsed = Double.parseDouble(String.valueOf((end-start)/1000));
        LOGGER.info(E.AMP+E.AMP+E.AMP+E.RED_APPLE + " createAggregatesForAllCities took " + elapsed
                + " seconds to calculate aggregates from Firestore " + E.RED_APPLE);
        return aggregates;
    }

}
