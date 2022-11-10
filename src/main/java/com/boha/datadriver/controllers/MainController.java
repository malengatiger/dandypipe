package com.boha.datadriver.controllers;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.CityPlace;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.models.Message;
import com.boha.datadriver.services.*;
import com.boha.datadriver.util.E;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
        } catch (Exception e) {
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
    @GetMapping("/getCitiesFromFile")
    private ResponseEntity<Object> getCitiesFromFile() {
        try {
            List<City> citiesFromFile = cityService.getCitiesFromFile();
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART + E.CHECK +
                    "  Cities from  file Found: " + citiesFromFile.size() + " " + E.CHECK);
            return ResponseEntity.ok(citiesFromFile);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
    @Autowired
    private StorageService storageService;
    @GetMapping("/getRecentEventsFromGCS")
    private ResponseEntity<Object> getRecentEventsFromGCS(@RequestParam int hours) {
        try {
            List<FlatEvent> events = storageService.getRecentFlatEvents(hours);
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART + E.CHECK +
                    "  Recent events from GCS Found: " + events.size() + " " + E.CHECK);
            return ResponseEntity.ok(events);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
    @GetMapping("/getLastEvent")
    private ResponseEntity<Object> getLastEvent(@RequestParam int hours) {
        try {
            Gson GSON = new GsonBuilder().setPrettyPrinting().create();
            FlatEvent event = eventService.getLastEvent(hours);
            LOGGER.info( E.CHECK + E.CHECK +
                    "  Last event from Firestore Found: " + GSON.toJson(event) + " " + E.CHECK);
            return ResponseEntity.ok(event);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
    @GetMapping("/generateCrowd")
    private ResponseEntity<Object> generateCrowd(@RequestParam String cityId, @RequestParam int total) {
        try {
            CityPlace place = generator.generateCrowd(cityId,total);
            LOGGER.info( E.CHECK + E.CHECK +
                    " Crowd generated: " + total + " at: " +place.name
                    + ",  " + place.cityName + E.CHECK);
            return ResponseEntity.ok("Crowd generated => "+ total
                    + " at: " +place.name + ", " + place.cityName);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
    @GetMapping("/countEvents")
    private ResponseEntity<Object> countEvents(@RequestParam int hours) {
        try {
            long count = eventService.countEvents(hours);
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART + E.CHECK +
                    "  Number of events from Firestore Found: " + count + " " + E.CHECK);
            return ResponseEntity.ok(count);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
    @Autowired
    private EventService eventService;
    @GetMapping("/getRecentEventsFromFirestore")
    private ResponseEntity<Object> getRecentEventsFromFirestore(@RequestParam int hours) {
        try {
            List<FlatEvent> events = eventService.getRecentEvents(hours);
            LOGGER.info(E.RED_APPLE + E.RED_APPLE + E.RED_APPLE +
                    "  Recent events from Firestore Found: " + events.size() + " " + E.CHECK);
            return ResponseEntity.ok(events);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
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
