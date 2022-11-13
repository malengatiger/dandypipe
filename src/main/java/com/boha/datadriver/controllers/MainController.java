package com.boha.datadriver.controllers;

import com.boha.datadriver.models.*;
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
    private String hello() {
        return E.BLUE_DOT + E.BLUE_DOT +
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

    @GetMapping("/generateUsers")
    private ResponseEntity<Object> generateUsers(@RequestParam int maxPerCityCount) {
        try {
            int count  = generator.generateUsers(maxPerCityCount);
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART +
                    " Users generated:  " + count);
            return ResponseEntity.ok("User generation completed: Generated:  " +
                    count);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @GetMapping("/generateCityUsers")
    private ResponseEntity<Object> generateCityUsers(@RequestParam String cityId,
                                                     @RequestParam int count) {
        try {
            int done = generator.generateCityUsers(cityId, count);
            City city  = cityService.getCityById(cityId);

            return ResponseEntity.ok(city.getCity()
                    + " users generation completed. Generated: " + done );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
    @GetMapping("/generateEventsByCity")
    private ResponseEntity<Object> generateEventsByCity(@RequestParam String cityId,
                                                        @RequestParam int count) {
        try {
            String done = generator.generateEventsByCity(cityId, count);
            City city  = cityService.getCityById(cityId);
            return ResponseEntity.ok(city.getCity()
                    + " event generation completed. Generated: " + done );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
    @GetMapping("/generateEventsByCities")
    private ResponseEntity<Object> generateEventsByCities(@RequestParam List<String> cityIds,
                                                        @RequestParam int upperCount) {
        try {
            List<String> done = generator.generateEventsByCities(cityIds, upperCount);
            return ResponseEntity.ok(done );
        } catch (Exception e) {
            return ResponseEntity.status(
                    HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }
    @GetMapping("/generateEventsByPlace")
    private ResponseEntity<Object> generateEventsByPlace(@RequestParam String placeId,
                                                     @RequestParam int count) {
        try {
            String done = generator.generateEventsByPlace(placeId, count);
            City city  = cityService.getCityById(placeId);

            return ResponseEntity.ok(city.getCity()
                    + " event generation by place completed. Generated: " + done );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
    @GetMapping("/generateEventsByPlaces")
    private ResponseEntity<Object> generateEventsByPlaces(@RequestParam List<String> placeIds,
                                                         @RequestParam int upperCount) {
        try {
            List<String> done = generator.generateEventsByPlaces(placeIds, upperCount);
            return ResponseEntity.ok(done );
        } catch (Exception e) {
            return ResponseEntity.status(
                    HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }

    @Autowired
    private UserService userService;

    @GetMapping("/countUsers")
    private ResponseEntity<Object> countUsers() {
        try {
            long count = userService.countUsers();
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART +
                    " Looks like city users counted: " + count);
            return ResponseEntity.ok(count);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }

    @GetMapping("/getCityUsers")
    private ResponseEntity<Object> getCityUsers(
            @RequestParam String cityId) {
        try {
            List<User> users = userService.getCityUsers(cityId);
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART  +
                    " Firestore Returning " + users.size() + " users " + E.CHECK);
            return ResponseEntity.ok(users);
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
            if (event != null) {
                LOGGER.info(E.CHECK + E.CHECK +
                        "  Last event from Firestore Found: " + GSON.toJson(event) + " " + E.CHECK);
                return ResponseEntity.ok(event);
            }  else {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body("Last event within " + hours + " hours not found");
            }
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

//    @GetMapping("/generateEvents")
//    private ResponseEntity<Message> generateEvents(long intervalInSeconds, int upperCountPerPlace, int maxCount) {
//        try {
//            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART +
//                    " MainController: ... Generator starting .... ");
//            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART +
//                    " MainController: intervalInSeconds:  " + intervalInSeconds +
//                    " upperCountPerPlace: " + upperCountPerPlace +
//                    " maxCount: " + maxCount + " " + E.RED_APPLE);
//
//            generator.generateEvents(intervalInSeconds,
//                    upperCountPerPlace, maxCount);
//            Message  message = new Message();
//            message.setStatusCode(200);
//            message.setMessage("Generator has started. Check logs Firestore, PubSub, BigQuery and GCS");
//            message.setDate(String.valueOf(new DateTime()));
//
//            return ResponseEntity.ok(
//                    message);
//        } catch (Exception e) {
//            e.printStackTrace();
//            Message  message = new Message();
//            message.setStatusCode(500);
//            message.setMessage(E.RED_DOT+
//                    "Something smells! Gone badly wrong: " + e.getMessage());
//            message.setDate(String.valueOf(new DateTime()));
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
//                    message);
//        }
//    }
//    @GetMapping("/stopGenerator")
//    private ResponseEntity<Message> generateEvents() {
//        generator.stopTimer();
//        Message  message = new Message();
//        message.setStatusCode(200);
//        message.setMessage("Generator has STOPPED. Check logs Firestore, PubSub, BigQuery and GCS");
//        message.setDate(String.valueOf(new DateTime()));
//
//        return ResponseEntity.ok(
//                message);
//    }

}
