package com.boha.datadriver.services;

import com.boha.datadriver.controllers.MainController;
import com.boha.datadriver.models.DashboardData;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.E;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

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

    public DashboardData getDashboardData(int minutesAgo) throws Exception {
        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT +
                "DashboardData is being built ... ");
        DashboardData dashboardData = new DashboardData();
        dashboardData.setMinutesAgo(minutesAgo);
        dashboardData.setUsers(userService.countUsers());
        dashboardData.setCities(cityService.countCities());
        dashboardData.setPlaces(placesService.countPlaces());
        dashboardData.setEvents(eventService.countEvents(minutesAgo));
        dashboardData.setDate(DateTime.now().toDateTimeISO().toString());
        dashboardData.setLongDate(DateTime.now().getMillis());

        List<FlatEvent> eventList = eventService.getRecentEvents(minutesAgo);
        double tot = 0.0;
        int totRating = 0;
        for (FlatEvent flatEvent : eventList) {
            tot += flatEvent.getAmount();
            totRating += flatEvent.getRating();
        }

        dashboardData.setAmount(tot);
        double avg = 0.00;
        if (eventList.size() > 0) {
             avg = Double.parseDouble("" + (totRating / eventList.size()));
        }

        dashboardData.setAverageRating(avg);
        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT +
                "DashboardData obtained: " + GSON.toJson(dashboardData));
        return dashboardData;
    }
}
