package com.boha.datadriver.services;

import com.boha.datadriver.models.DashboardData;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.DB;
import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import com.google.firebase.cloud.FirestoreClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class DashboardService {
    final UserService userService;
    final CityService cityService;
    final EventService eventService;
    final PlacesService placesService;
    private static final Logger LOGGER = Logger.getLogger(DashboardService.class.getSimpleName());
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();


    public DashboardService(UserService userService, CityService cityService, EventService eventService, PlacesService placesService) {
        this.userService = userService;
        this.cityService = cityService;
        this.eventService = eventService;
        this.placesService = placesService;
    }

    public List<DashboardData> getDashboardData(int minutesAgo) throws Exception {
        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT +
                "DashboardData is being built ... ");
        long start = System.currentTimeMillis();
        List<DashboardData> list = new ArrayList<>();
        Firestore c = FirestoreClient.getFirestore();
        DateTime dt = DateTime.now().toDateTimeISO().minusMinutes(minutesAgo);
        QuerySnapshot snapshot = c.collection(DB.dashboards)
                .whereGreaterThanOrEqualTo("longDate", dt.getMillis())
                .orderBy("longDate", Query.Direction.DESCENDING)
                .get().get();

        for (QueryDocumentSnapshot doc : snapshot.getDocuments()) {
            DashboardData data = doc.toObject(DashboardData.class);
            list.add(data);
        }

        long end = System.currentTimeMillis();
        LOGGER.info(E.BLUE_DOT + E.RED_DOT +
                "DashboardData query took " + (end - start) + " milliseconds, found: " + list.size());

        return list;
    }

    private MyBag myBag;

    public DashboardData addDashboardData(int minutesAgo) throws Exception {
        Firestore c = FirestoreClient.getFirestore();
        long start = System.currentTimeMillis();
        DashboardData dashboardData = new DashboardData();
        dashboardData.setMinutesAgo(minutesAgo);
        dashboardData.setUsers(userService.countUsers());
        dashboardData.setCities(cityService.countCities());
        dashboardData.setPlaces(placesService.countPlaces());
        dashboardData.setEvents(eventService.countEvents(minutesAgo));
        dashboardData.setDate(DateTime.now().toDateTimeISO().toString());
        dashboardData.setLongDate(DateTime.now().getMillis());
        long end = System.currentTimeMillis();
        double elapsed = Double.parseDouble(String.valueOf((end - start) / 1000));
        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT +
                "DashboardData has counted everything: " + elapsed + " seconds elapsed. " +
                "... calculating totals and averages");

        long start2 = System.currentTimeMillis();
        List<FlatEvent> eventList = eventService.getRecentEvents(minutesAgo);
        long endX = System.currentTimeMillis();
        double elapsedGetEvents = Double.parseDouble(String.valueOf((endX - start2) / 1000));
        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT +
                "Events data took this much time - seconds elapsed: " + elapsedGetEvents + " " + E.RED_DOT);
        long start3 = System.currentTimeMillis();
        double tot = 0.0;
        int totRating = 0;
        for (FlatEvent flatEvent : eventList) {
            tot += flatEvent.getAmount();
            totRating += flatEvent.getRating();
        }

        dashboardData.setAmount(tot);
        double avg = 0.0;
        if (eventList.size() > 0) {
            Double totRatingDouble = Double.parseDouble("" + totRating);
            Double eventsDouble = Double.parseDouble("" + eventList.size());
            avg = totRatingDouble / eventsDouble;
        }

        dashboardData.setAverageRating(avg);

        long end2 = System.currentTimeMillis();
        double elapsed2 = Double.parseDouble(String.valueOf((end2 - start3) / 1000));
        LOGGER.info(E.BLUE_DOT + E.RED_DOT +
                "DashboardData calculating took " + elapsed2 + " seconds; avg rating: " + avg);

        elapsed2 += elapsed + elapsedGetEvents;
        dashboardData.setElapsedSeconds(elapsed2);

        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT +
                "DashboardData obtained: " + elapsed2 + " total seconds elapsed. " + GSON.toJson(dashboardData));


        ApiFuture<DocumentReference> future = c.collection(DB.dashboards).add(dashboardData);

        LOGGER.info(E.GREEN_APPLE + " Firestore dashboardData added; path: " + future.get().getPath());

        return dashboardData;
    }


}


