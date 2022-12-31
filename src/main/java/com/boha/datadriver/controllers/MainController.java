package com.boha.datadriver.controllers;

import com.boha.datadriver.models.*;
import com.boha.datadriver.services.*;
import com.boha.datadriver.util.E;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Logger;

/**
 * Manages the DataDriver API. All public app routes are defined here
 */
@RestController
public class MainController {
    private static final Logger LOGGER = Logger.getLogger(MainController.class.getSimpleName());

    public MainController(FileService fileService, CacheService cacheService, CityService cityService,
                          DashboardService dashboardService, CityAggregateService cityAggregateService) {
        this.fileService = fileService;
        this.cacheService = cacheService;
        this.cityService = cityService;
        this.dashboardService = dashboardService;
        this.cityAggregateService = cityAggregateService;
    }


    final FileService fileService;
    final CacheService cacheService;
    private final CityService cityService;
    private final DashboardService dashboardService;
    private final CityAggregateService cityAggregateService;


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
            LOGGER.info("generateEventsByCity request come in: "+E.BLUE_DOT+ " "+cityId
                    + " count: " + count+ " " + E.RED_DOT);
        try {
            GenerationMessage done = generator.generateEventsByCity(cityId, count);
//            LOGGER.info("Events generated: " +
//                    new GsonBuilder().setPrettyPrinting().create().toJson(done));
            return ResponseEntity.ok(done );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @GetMapping("/getCityAggregates")
    private ResponseEntity<Object> getCityAggregates(@RequestParam int minutesAgo) {
        try {
            List<CityAggregate> done = cityAggregateService.getCityAggregates(minutesAgo);
            return ResponseEntity.ok(done );
        } catch (Exception e) {
            return ResponseEntity.status(
                    HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }
    @GetMapping("/getCityAggregatesByCity")
    private ResponseEntity<Object> getCityAggregatesByCity(@RequestParam String cityId, @RequestParam int minutesAgo) {
        try {
            List<CityAggregate> done = cityAggregateService.getCityAggregatesByCity(cityId,minutesAgo);
            return ResponseEntity.ok(done );
        } catch (Exception e) {
            return ResponseEntity.status(
                            HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }

    @GetMapping("/getCityEvents")
    private ResponseEntity<Object> getCityEvents(@RequestParam String cityId, @RequestParam int minutes) {
        try {
            List<FlatEvent> done = cityService.getCityEvents(cityId,minutes);
            return ResponseEntity.ok(done );
        } catch (Exception e) {
            return ResponseEntity.status(
                            HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }

    @GetMapping("/getPlaceAggregate")
    private ResponseEntity<Object> getPlaceAggregate(@RequestParam String placeId, @RequestParam int minutes) {
        try {
            PlaceAggregate done = placesService.getPlaceAggregate(placeId, minutes);
            return ResponseEntity.ok(done );
        } catch (Exception e) {
            return ResponseEntity.status(
                            HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }
    @GetMapping("/generateEventsByCities")
    private ResponseEntity<Object> generateEventsByCities(@RequestParam List<String> cityIds,
                                                          @RequestParam int upperCount) {
        try {
            List<GenerationMessage> done = generator.generateEventsByCities(cityIds, upperCount);
            return ResponseEntity.ok(done );
        } catch (Exception e) {
            return ResponseEntity.status(
                            HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }
    @GetMapping("/generateEventsByPlace")
    private ResponseEntity<Object> generateEventsByPlace(@RequestParam String placeId,
                                                     @RequestParam int count, @RequestParam boolean isBad) {
        try {
            GenerationMessage done = generator.generateEventsByPlace(placeId, count, isBad);
            return ResponseEntity.ok(done );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @GetMapping("/getDashboardData")
    private ResponseEntity<Object> getDashboardData( @RequestParam int minutesAgo) {
        try {
            List<DashboardData> data = dashboardService.getDashboardData( minutesAgo);
            return ResponseEntity.ok(data );
        } catch (Exception e) {
            return ResponseEntity.status(
                    HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }
    @GetMapping("/generateData")
    private ResponseEntity<Object> generateData(@RequestParam int upperCount) {
        try {
            GenerationResultsBag data = generator.generateData(upperCount);
            return ResponseEntity.ok(data );
        } catch (Exception e) {
            return ResponseEntity.status(
                            HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }
    @GetMapping(value = "/getDataForCache")
    private ResponseEntity<Object> getDataForCache(@RequestParam int minutesAgo) {
        try {
            CacheBag data = cacheService.getDataForCache(minutesAgo);
            return ResponseEntity.ok(data );
        } catch (Exception e) {
            return ResponseEntity.status(
                            HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }
    @GetMapping(value = "/getEventsForCache")
    private ResponseEntity<Object> getEventsForCache(@RequestParam String cityId,@RequestParam int minutesAgo) {
        try {
            List<FlatEvent> data = cacheService.getCityEvents(cityId,minutesAgo);
            return ResponseEntity.ok(data );
        } catch (Exception e) {
            return ResponseEntity.status(
                            HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }
    @GetMapping("/addDashboardData")
    private ResponseEntity<Object> addDashboardData( @RequestParam int minutesAgo) {
        try {
            DashboardData data = dashboardService.addDashboardData( minutesAgo);
            return ResponseEntity.ok(data );
        } catch (Exception e) {
            return ResponseEntity.status(
                            HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
        }
    }

    @GetMapping("/files")
    @ResponseBody
    public ResponseEntity<Resource> getFile(@RequestParam String filename) throws Exception {
        Resource file = fileService.getFileResource(filename);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getFilename() + "\"").body(file);
    }
    @GetMapping(
            value = "/getFileStream",
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE
    )

    public @ResponseBody byte[] getFileStream(@RequestParam String filename) throws Exception {
        Resource file = fileService.getFileResource(filename);
        InputStream in = file.getInputStream();
        byte[] bytes = IOUtils.toByteArray(in);
        LOGGER.info(E.LEAF+E.LEAF+" Sending bytes: "
                + bytes.length + " comprising file: " + file.getFile().getPath());
        return bytes;
    }

    @GetMapping("/getEventZippedFilePath")
    private ResponseEntity<Object> getEventZippedFile( @RequestParam String cityId, @RequestParam int minutesAgo) throws Exception {
        try {
            String zippedFilePath = cacheService.getEventZippedFilePath(cityId, minutesAgo);
            LOGGER.info(E.AMP+E.AMP+E.AMP+E.AMP+" returning Zipped file path: " + zippedFilePath);
            return ResponseEntity.ok(zippedFilePath);
        } catch (Exception e) {
            LOGGER.severe(" We have a problem with a file");
            return ResponseEntity.badRequest().body(e.getMessage());

        }
    }
    @GetMapping("/deleteTemporaryFiles")
    private ResponseEntity<Object> deleteTemporaryFiles() throws Exception {
        try {
            int data = fileService.deleteTemporaryFiles();
            return ResponseEntity.ok("Temporary files deleted: " + data);
        } catch (Exception e) {
            LOGGER.severe(" We have a problem with a file");
            return ResponseEntity.badRequest().body(e.getMessage());

        }
    }
    @GetMapping("/createAggregatesForAllCities")

    private ResponseEntity<Object> createAggregatesForAllCities( @RequestParam int minutesAgo) {
        try {
            List<CityAggregate> data = cityAggregateService.createAggregatesForAllCities( minutesAgo);
            return ResponseEntity.ok(data );
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
            List<City> cities = cityService.getCities();
            LOGGER.info(E.BLUE_HEART + E.BLUE_HEART + E.CHECK +
                    " Firestore Returning " + cities.size() + " cities " + E.CHECK);
            return ResponseEntity.ok(cities);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(e.getMessage());
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
    @GetMapping("/getPlaces")
    private ResponseEntity<Object> getPlaces() {
        try {
            List<CityPlace> placesByCity = placesService.getPlaces();
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
    private ResponseEntity<Object> generateCrowd(@RequestParam String cityId,
                                                 @RequestParam int total,
                                                 @RequestParam boolean isBad) {
        try {
            CityPlace place = generator.generateCrowd(cityId,total, isBad);
            LOGGER.info( E.CHECK + E.CHECK +
                    " Crowd generated: " + total + " at: " +place.getName()
                    + ",  " + place.getCityName() + E.CHECK);
            return ResponseEntity.ok("Crowd generated => "+ total
                    + " at: " +place.getName() + ", " + place.getCityName());
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


}
