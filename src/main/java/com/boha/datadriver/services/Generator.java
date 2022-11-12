package com.boha.datadriver.services;

import com.boha.datadriver.models.*;
import com.boha.datadriver.util.E;
import com.boha.datadriver.util.FlatEventGetter;
import com.boha.datadriver.util.LogControl;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.firebase.cloud.FirestoreClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.logging.Logger;

/**
 * Manages the creation of streaming data to PubSub and Firestore. Events are generated and sent forth
 */
@Service
//@EnableScheduling
public class Generator {
    static final Logger LOGGER = Logger.getLogger(Generator.class.getSimpleName());
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();


    public Generator(CityService cityService, PlacesService placesService, EventPublisher eventPublisher) {
        this.cityService = cityService;
        this.placesService = placesService;
        this.eventPublisher = eventPublisher;
    }

    @Autowired
    private LogControl logControl;


    private final ArrayList<Subscriber> allSubscribers = new ArrayList<>();
    private final CityService cityService;
    private final PlacesService placesService;
    private final EventPublisher eventPublisher;

    @Autowired
    private EventSubscriber eventSubscriber;

    private static Random random = new Random(System.currentTimeMillis());
    static Timer timer;
    static List<City> cityList;
    static int totalCount = 0;
    static int maxCount;

    static long startTime;

    int searchCount = 0;

    private User getUnusedRandomUser(String cityId) throws Exception {
        User user;
        List<User> users = getUsers(cityId);
        int index = random.nextInt(users.size() - 1);
        user = users.get(index);
        if (userMap.containsKey(user.getUserId())) {
            searchCount++;
            if (searchCount > users.size()) {
                String msg = "Unable to find unused random user";
                LOGGER.info(E.RED_DOT + E.RED_DOT + " " + msg);
                throw new Exception(msg);
            }
            user = getUnusedRandomUser(cityId);
        } else {
            userMap.put(user.getUserId(), user);
            searchCount = 0;
        }

        return user;
    }

    HashMap<String, User> userMap = new HashMap<>();

    private void generateRandomCrowd(CityPlace place) throws Exception {
        userMap = new HashMap<>();
        long startTime = DateTime.now().getMillis();
        int count = random.nextInt(400);
        if (count < 100) count = 100;

        int done = 0;
        for (int i = 0; i < count; i++) {
            try {
                done += generateEventAtPlace(place, getUnusedRandomUser(place.cityId));
            } catch (Exception e) {
                LOGGER.info(E.RED_DOT + E.RED_DOT + " " + e.getMessage());
            }
        }


        logControl.info(E.BLUE_DOT + E.BLUE_DOT + E.BLUE_DOT +
                " Random crowd: " + done + " events at: "
                + place.name + ", " + place.cityName);
        long end = DateTime.now().getMillis();
        logControl.info(E.BLUE_DOT + E.BLUE_DOT + E.BLUE_DOT +
                " Elapsed time: " + ((end - startTime)) + " milliseconds for generating random crowd");


    }

    public CityPlace generateCrowd(String cityId, int total) throws Exception {

        long start = DateTime.now().getMillis();
        totalCount = 0;
        pubSubCount = 0;
        List<CityPlace> places;
        try {
            places = placesService.getPlacesByCity(cityId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        int index = random.nextInt(places.size() - 1);
        CityPlace place = places.get(index);
        if (place.name.trim().equalsIgnoreCase(place.cityName.trim())) {
            LOGGER.info(E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR
                    + " Ignore this place for crowd generation: " + place.name);
            generateCrowd(cityId, total);
        }
//        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR+E.RED_APPLE +
//                " Generating crowd: " + place.name + ", " + place.cityName);
        int successCount = 0;
        for (int i = 0; i < total; i++) {
            try {
                successCount += generateEventAtPlace(place, getUnusedRandomUser(place.cityId));
            } catch (Exception e) {
                LOGGER.info(E.RED_DOT + E.RED_DOT + " " + e.getMessage());
            }
        }

        LOGGER.info(E.YELLOW_STAR + E.YELLOW_STAR
                + " Done generating a crowd of " + successCount + " PubSub Calls: " + pubSubCount
                + E.PEAR + " at: "
                + place.name + ", " + place.cityName + E.RED_APPLE);
        long end = DateTime.now().getMillis();
        LOGGER.info(E.PEAR + E.PEAR +
                " Elapsed time: " + ((end - start)) + " milliseconds for generating crowd");
        totalCount = 0;
        return place;
    }

    long start;

    public void generateEvents(long intervalInSeconds,
                               int upperCountPerPlace, int maximumCount) throws Exception {
        maxCount = maximumCount;
        pubSubCount = 0;
        totalCount = 0;
        start = DateTime.now().getMillis();
        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT + E.BLUE_DOT + E.BLUE_DOT + E.BLUE_DOT +
                " Generator: intervalInSeconds: " + intervalInSeconds
                + " upperCountPerPlace: " +
                +upperCountPerPlace + " maxCount: " + maxCount + " "
                + E.BLUE_DOT + E.BLUE_DOT);
        if (cityList == null || cityList.isEmpty()) {
            cityList = cityService.getCitiesFromFirestore();
        }
        LOGGER.info(E.BLUE_DOT + E.BLUE_DOT + " starting Timer to control work ...");
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    performWork(upperCountPerPlace);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, 1000, intervalInSeconds * 1000);

    }

    public void stopTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;

            LOGGER.info(E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR +
                    " Generator Timer stopped; events: " + E.LEAF
                    + " totalCount: " + totalCount + " " +E.AMP + " PubSub count: " + pubSubCount);
            totalCount = 0;
            pubSubCount = 0;
            long end = DateTime.now().getMillis();
            if (((end - start) / 1000 / 60) == 0) {
                LOGGER.info(E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR +
                        " Elapsed time: " + E.LEAF + ((end - start) / 1000)
                        + " seconds for generating events");
            } else {
                LOGGER.info(E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR + E.YELLOW_STAR +
                        " Elapsed time: " + E.LEAF + " "
                        + ((end - start) / 1000 / 60) + " minutes for generating events");
            }
        }
    }

    private void performWork(int upperCountPerPlace) throws Exception {
        int index = random.nextInt(cityList.size() - 1);
        City city = cityList.get(index);
        List<CityPlace> places;
        try {
            places = placesService.getPlacesByCity(city.getId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int count = random.nextInt(upperCountPerPlace);
        if (count < 25) count = 50;

        int realCount = 0;
        for (int i = 0; i < count; i++) {
            int mIndex = random.nextInt(places.size() - 1);
            CityPlace cityPlace = places.get(mIndex);
            try {
                realCount += generateEventAtPlace(cityPlace,
                        getUnusedRandomUser(city.getId()));
            } catch (Exception e) {
                LOGGER.info(E.RED_DOT + E.RED_DOT + " " + e.getMessage());
            }
        }

        int chooser = random.nextInt(100);
        int mIndex = random.nextInt(places.size() - 1);
        CityPlace cityPlace = places.get(mIndex);
        if (chooser < 15) {
            generateRandomCrowd(cityPlace);
        }

        logControl.info(E.LEAF + E.LEAF + " Events generated: count: " + realCount + " " +
                E.RED_APPLE + " totalCount: " + totalCount + " for city: " + E.PEAR
                + " " + city.getCity());
        if (totalCount > maxCount) {
            stopTimer();
            totalCount = 0;
        }
    }

    int pubSubCount;

    private int generateEventAtPlace(CityPlace cityPlace, User user) throws Exception {

        Event event;
        try {
            event = getEvent(cityPlace, user);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!event.getCityPlace().cityName.trim().equalsIgnoreCase(event.getCityPlace().name.trim())) {
            event.setDate(DateTime.now().toDateTimeISO().toString());
            event.setLongDate(DateTime.now().getMillis());
            try {
                writeEventToFirestore(event);
                sendToPubSub(event);
            } catch (Exception e) {
                LOGGER.info(E.RED_DOT + E.RED_DOT + E.RED_DOT + " Problemo Senor? " + e.getMessage());
                e.printStackTrace();
            }
            totalCount++;
            int rem = totalCount % 1000;
            if (rem == 0) {
                LOGGER.info(E.RED_APPLE + E.RED_APPLE +
                        " Total Count of Events Generated: " + totalCount);
            }
            return 1;
        } else {
            return 0;
        }
    }

    private List<User> getUsers(String cityId) throws Exception {
        List<User> users;
        if (!hashMap.containsKey(cityId)) {
            users = userService.getCityUsers(cityId);
            hashMap.put(cityId, users);
        } else {
            users = hashMap.get(cityId);
        }
        return users;
    }

    private Event getEvent(CityPlace cityPlace, User user) {
        if (user == null) {
            String msg = "User not available for event";
            LOGGER.info(E.RED_DOT+E.RED_DOT+ " " + msg);
            throw new RuntimeException(msg);
        }
        Event event = new Event();
        event.setCityPlace(cityPlace);
        int m = random.nextInt(2500);
        if (m == 0) m = 150;
        int cents = random.nextInt(99);
        if (cents < 10) cents = 50;
        event.setAmount(Double.parseDouble("" + m + "." + cents));
        int r = random.nextInt(5);
        if (r == 0) r = 5;
        event.setRating(r);
        event.setDate(DateTime.now().toDateTimeISO().toString());
        event.setLongDate(DateTime.now().getMillis());
        event.setEventId(UUID.randomUUID().toString());
        event.setUser(user);
        return event;
    }

    Firestore firestore;

    private void writeEventToFirestore(Event event) throws Exception {
        if (firestore == null) {
            firestore = FirestoreClient.getFirestore();
        }
        FlatEvent flatEvent = FlatEventGetter.getFlatEvent(event);
        ApiFuture<DocumentReference> future =
                firestore.collection("flatEvents").add(flatEvent);

    }

    private void sendToPubSub(Event event) throws Exception {

        eventPublisher.publishEvent(GSON.toJson(event));
        FlatEvent fe = FlatEventGetter.getFlatEvent(event);
        eventPublisher.publishFlatEvent(GSON.toJson(fe));
        eventPublisher.publishBigQueryEvent(GSON.toJson(fe));
        eventPublisher.publishPull(GSON.toJson(fe));

        pubSubCount += 4;
        int rem = pubSubCount % 4000;
        if (rem == 0) {
            LOGGER.info(E.ORANGE_HEART+E.ORANGE_HEART+
                    " Total PubSub publishing calls: " + E.AMP + pubSubCount +
                    " to 4 topics");
        }

    }

    @Autowired
    private UserService userService;
    @Value("${surnames}")
    private String surnamesFile;
    @Value("${firstNames}")
    private String firstNamesFile;

    private int userTotal;

    public int generateCityUsers(String cityId, int count) throws Exception {

        if (lastNames.size() == 0) {
            setLastNames();
            setFirstNames();
            setMiddleInitials();
        }
        userTotal = 0;
        City city = cityService.getCityById(cityId);
        int done = processCityUsers(count, city);
        return done;
    }

    public int generateUsers(int maxPerCityCount) throws Exception {
        if (cityList == null || cityList.isEmpty()) {
            cityList = cityService.getCitiesFromFirestore();
        }
        userTotal = 0;
        if (lastNames.size() == 0) {
            setLastNames();
            setFirstNames();
            setMiddleInitials();
        }

        for (City city : cityList) {
            int count = random.nextInt(maxPerCityCount);
            if (count < 100) count = 120;
            processCityUsers(count, city);
        }
        LOGGER.info(E.RED_DOT +
                " Users generated for " + cityList.size() + " cities:  "
                + E.RED_APPLE + " " + userTotal);

        return userTotal;
    }

    private int processCityUsers(int count, City city) {

        int cityCount = 0;
        for (int i = 0; i < count; i++) {
            int index = random.nextInt(firstNames.size() - 1);
            String firstName = firstNames.get(index);
            index = random.nextInt(lastNames.size() - 1);
            String lastName = lastNames.get(index);
            index = random.nextInt(middleInitials.size() - 1);
            String initial = middleInitials.get(index);
            User user = new User();
            user.setUserId(UUID.randomUUID().toString());
            user.setCityId(city.getId());
            user.setCityName(city.getCity());
            user.setFirstName(firstName);
            user.setLastName(lastName);
            user.setMiddleInitial(initial);
            user.setDateRegistered(DateTime.now().toDateTimeISO().toString());
            user.setLongDateRegistered(DateTime.now().getMillis());
            try {
                userService.addUser(user);
                cityCount++;
                userTotal++;
            } catch (Exception e) {
                LOGGER.info(" Problem: " + e.getMessage());
            }
        }
        LOGGER.info(E.LEAF + E.LEAF +
                " Generated " + cityCount + " users for " +
                city.getCity());
        return cityCount;
    }

    private final HashMap<String, List<User>> hashMap = new HashMap<>();

    public String generateEventsByCity(String cityId, int count) throws Exception {

        City city = cityService.getCityById(cityId);
        List<CityPlace> places = placesService.getPlacesByCity(cityId);
        userMap = new HashMap<>();
        totalCount = 0;
        int total = 0;
        for (int i = 0; i < count; i++) {
            int placeIndex = random.nextInt(places.size() - 1);
            CityPlace place = places.get(placeIndex);
            try {
                total += generateEventAtPlace(place, getUnusedRandomUser(cityId));

            } catch (Exception e) {
                LOGGER.info(E.RED_DOT + E.RED_DOT + " " + e.getMessage());
            }

        }
        String msg = " events generated: " + total + " city: " + city.getCity();
        LOGGER.info(E.RED_APPLE + " " + msg);
        return msg;
    }

    public String generateEventsByPlace(String placeId, int count) throws Exception {
        CityPlace place = placesService.getPlaceById(placeId);
        int total = 0;
        totalCount = 0;
        userMap = new HashMap<>();
        for (int i = 0; i < count; i++) {
            try {
                total += generateEventAtPlace(place, getUnusedRandomUser(place.cityId));
            } catch (Exception e) {
                LOGGER.info(E.RED_DOT + E.RED_DOT + " " + e.getMessage());
            }

        }
        String msg = " events generated: " + total + " at " + place.name;
        LOGGER.info(E.RED_APPLE + " " + msg);
        return msg;

    }

    private final List<String> firstNames = new ArrayList<>();
    private final List<String> lastNames = new ArrayList<>();
    private final List<String> middleInitials = new ArrayList<>();

    @Autowired
    private StorageService storageService;

    private void setLastNames() {
        String mSurnames = storageService.downloadObject(surnamesFile);
        JSONArray arr = new JSONArray(mSurnames);
        for (int i = 0; i < arr.length(); i++) {
            JSONObject obj = arr.getJSONObject(i);
            String surname = obj.getString("surname");
            String real = processName(surname);
            lastNames.add(real);
        }
        LOGGER.info(E.LEAF + E.LEAF +
                " Number of LastNames: " + lastNames.size());
    }

    private String processName(String name) {
        String first = name.substring(0, 1);
        String capFirst = first.toUpperCase();
        String rest = name.substring(1);
        String lowRest = rest.toLowerCase();
        return capFirst + lowRest;
    }

    private void setFirstNames() {
        String mFirstNames = storageService.downloadObject(firstNamesFile);
        JSONArray arr = new JSONArray(mFirstNames);
        for (int i = 0; i < arr.length(); i++) {
            JSONObject obj = arr.getJSONObject(i);
            String firstName = obj.getString("name");
            String real = processName(firstName);
            firstNames.add(real);
        }
        LOGGER.info(E.LEAF + E.LEAF +
                " Number of FirstNames: " + firstNames.size());


    }

    private void setMiddleInitials() {
        middleInitials.add("A");
        middleInitials.add("B");
        middleInitials.add("C");
        middleInitials.add("D");
        middleInitials.add("E");
        middleInitials.add("F");
        middleInitials.add("G");
        middleInitials.add("H");
        middleInitials.add("J");
        middleInitials.add("K");
        middleInitials.add("L");
        middleInitials.add("M");
        middleInitials.add("N");
        middleInitials.add("O");
        middleInitials.add("P");
        middleInitials.add("Q");
        middleInitials.add("R");
        middleInitials.add("S");
        middleInitials.add("T");
        middleInitials.add("X");
        middleInitials.add("Z");

    }
}
