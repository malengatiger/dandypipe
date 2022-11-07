package com.boha.datadriver.controllers;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.CityPlace;
import com.boha.datadriver.models.Message;
import com.boha.datadriver.services.CityService;
import com.boha.datadriver.services.Generator;
import com.boha.datadriver.services.PlacesService;
import com.boha.datadriver.util.E;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Manages the DataDriver API. All public app routes are defined here
 */
@RestController
public class MainController {
    private static final Logger LOGGER = Logger.getLogger(MainController.class.getSimpleName());

    public MainController(CityService cityService) {
        this.cityService = cityService;
    }


    private final CityService cityService;

    @GetMapping("/")
    private String hello()  {
        return E.BLUE_DOT+E.BLUE_DOT+
                "DataDriver is running at " + new DateTime().toDateTimeISO().toString();
    }

    @GetMapping("/saveCities")
    private ResponseEntity<Object> saveCities() {
        try {
            List<City> cities = cityService.addCitiesToFirestore();
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART + E.CHECK +
                    " MainController Returning " + cities.size() + " cities");
            return ResponseEntity.ok(cities);
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e);
        }
    }

    @GetMapping("/getCities")
    private ResponseEntity<Object> getCities() {
        try {
            List<City> cities = cityService.getCitiesFromFirestore();
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART + E.CHECK +
                    " Firestore Returning " + cities.size() + " cities " + E.CHECK);
            return ResponseEntity.ok(cities);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e);
        }
    }

    @Autowired
    PlacesService placesService;

    @GetMapping("/loadCityPlaces")
    private ResponseEntity<Object> loadCityPlaces() {
        try {
            String loaded = placesService.loadCityPlaces();
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART + E.CHECK +
                    "  " + loaded + E.CHECK);
            return ResponseEntity.ok(loaded);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e);
        }
    }

    @GetMapping("/getPlacesByCity")
    private ResponseEntity<Object> getPlacesByCity(@RequestParam String cityId) {
        try {
            List<CityPlace> placesByCity = placesService.getPlacesByCity(cityId);
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART + E.CHECK +
                    "  City Places Found: " + placesByCity.size() + " " + E.CHECK);
            return ResponseEntity.ok(placesByCity);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e);
        }
    }

    @Autowired
    Generator generator;

    @GetMapping("/generateEvents")
    private ResponseEntity<Message> generateEvents(long intervalInSeconds, int upperCountPerPlace, int maxCount) {
        try {
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART +
                    " MainController: ... Generator starting .... ");
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART +
                    " MainController: intervalInSeconds:  " + intervalInSeconds +
                    " upperCountPerPlace: " + upperCountPerPlace +
                    " maxCount: " + maxCount + " " + E.RED_APPLE);

            generator.generateEvents(intervalInSeconds,
                    upperCountPerPlace, maxCount);
            Message  message = new Message();
            message.setStatusCode(200);
            message.setMessage("Generator has started. Check logs Firestore, PubSub, BigQuery and GCS");
            message.setDate(String.valueOf(new DateTime()));

            return ResponseEntity.ok(
                    message);
        } catch (Exception e) {
            e.printStackTrace();
            Message  message = new Message();
            message.setStatusCode(500);
            message.setMessage(E.RED_DOT+
                    "Something smells! Gone badly wrong: " + e.getMessage());
            message.setDate(String.valueOf(new DateTime()));
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                    message);
        }
    }
    @GetMapping("/stopGenerator")
    private ResponseEntity<Message> generateEvents() {
        generator.stopTimer();
        Message  message = new Message();
        message.setStatusCode(200);
        message.setMessage("Generator has STOPPED. Check logs Firestore, PubSub, BigQuery and GCS");
        message.setDate(String.valueOf(new DateTime()));

        return ResponseEntity.ok(
                message);
    }

}
