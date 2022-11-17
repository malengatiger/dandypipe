package com.boha.datadriver.services;

import com.boha.datadriver.models.*;
import com.boha.datadriver.util.E;
import com.boha.datadriver.util.FlatEventGetter;
import com.boha.datadriver.util.LogControl;
import com.boha.datadriver.util.WriteLogEntry;
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


    public Generator(CityService cityService, PlacesService placesService, EventPublisher eventPublisher, FirebaseService firebaseService) {
        this.cityService = cityService;
        this.placesService = placesService;
        this.eventPublisher = eventPublisher;
        this.firebaseService = firebaseService;
        try {
            //this.firebaseService.initializeFirebase();
        } catch (Exception  e)  {
            LOGGER.info(" Firebase initialization failed");
        }
    }
// c0751f57-2493-47f8-b8a6-664637992db5, b07ae3a-3105-483f-a023-c3e359241d05, b5aa2593-b333-4c84-82c6-d10d6784b9d8, edc4cab4-38f0-4e4c-882a-813597277936, 253c0404-3458-480c-bc51-cf0e020b28b4, 151d3b43-de61-4c1f-a470-e62f310cdf7f, 18c05582-ab79-4073-9bc8-21e452f0bd9e, 82bc129f-701e-435f-8d32-9dbdec025407, ad01a09e-f865-424f-9804-cd248733eb81, d955d30b-9903-45c1-a466-db7bfad385d8
    @Autowired
    private LogControl logControl;
    @Autowired
    private WriteLogEntry writeLogEntry;


    private final ArrayList<Subscriber> allSubscribers = new ArrayList<>();
    private final CityService cityService;
    private final PlacesService placesService;
    private final EventPublisher eventPublisher;
    private final FirebaseService firebaseService;

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

    private void generateRandomCrowd(CityPlace place, boolean isBad) throws Exception {
        userMap = new HashMap<>();
        long startTime = DateTime.now().getMillis();
        int count = random.nextInt(1000);
        if (count < 200) count = 450;

        int done = 0;
        for (int i = 0; i < count; i++) {
            try {
                done += generateEventAtPlace(place, getUnusedRandomUser(place.cityId), isBad);
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

    public CityPlace generateCrowd(String cityId, int total, boolean isBad) throws Exception {

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
            generateCrowd(cityId, total, isBad);
        }
        int successCount = 0;
        for (int i = 0; i < total; i++) {
            try {
                successCount += generateEventAtPlace(place, getUnusedRandomUser(place.cityId), isBad);
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

    int pubSubCount;

    private int generateEventAtPlace(CityPlace cityPlace, User user, boolean isBad) throws Exception {


        Event event;
        try {
            event = getEvent(cityPlace, user, isBad);
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
            int rem = totalCount % 8000;
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

    private Event getEvent(CityPlace cityPlace, User user, boolean isBad) {
        if (user == null) {
            String msg = "User not available for event";
            LOGGER.info(E.RED_DOT + E.RED_DOT + " " + msg);
            throw new RuntimeException(msg);
        }
        Event event = new Event();
        event.setCityPlace(cityPlace);
        int m = random.nextInt(2500);
        if (m == 0) m = 100;
        int cents = random.nextInt(99);
        if (cents < 10) cents = 50;
        event.setAmount(Double.parseDouble("" + m + "." + cents));
        event.setRating(isBad? getBadRating() : getGoodRating());
        event.setDate(DateTime.now().toDateTimeISO().toString());
        event.setLongDate(DateTime.now().getMillis());
        event.setEventId(UUID.randomUUID().toString());
        event.setUser(user);
        return event;
    }
    int getGoodRating() {
        int m = random.nextInt(100);
        if (m <= 10) {
            return 3;
        }
        if (m <= 70) {
            return 4;
        }

        return 5;
    }
    int getBadRating() {
        int m = random.nextInt(100);
        if (m <= 20) {
            return 1;
        }
        if (m <= 80) {
            return 2;
        }
        if (m <= 90) {
            return 3;
        }

        return 1;
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
        int rem = pubSubCount % 8000;
        if (rem == 0) {
            LOGGER.info(E.ORANGE_HEART + E.ORANGE_HEART +
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

    public List<GenerationMessage> generateEventsByCities(List<String> cityIds, int upperCount) throws Exception {
        List<GenerationMessage> messages = new ArrayList<>();
        totalCount = 0;

        for (String cityId : cityIds) {
            int count = random.nextInt(upperCount);
            if (count < 200) count = 400;
            try {
                messages.add(generateEventsByCity(cityId, count));
            } catch (Exception e) {
                LOGGER.info(E.RED_DOT+" Error: " + e.getMessage());
            }
        }
        int total = 0;
        for (GenerationMessage message : messages) {
            total +=  message.getCount();
        }
        LOGGER.info(E.LEAF+E.LEAF+" Total Cities generated: " + messages.size());

        Collections.sort(messages);
        GenerationMessage msg = new GenerationMessage();
        msg.setType("generateEventsByCities");
        msg.setCount(total);
        msg.setMessage("Total Events");
        messages.add(msg);
        return messages;
    }
    public List<GenerationMessage> generateEventsByPlaces(List<String> placeIds, int upperCount) throws Exception {
        List<GenerationMessage> messages = new ArrayList<>();
        boolean isBad;
        int m = random.nextInt(100);
        isBad = m <= 40;
        for (String placeId : placeIds) {
            int count = random.nextInt(upperCount);
            if (count < 100) count = 200;
            messages.add(generateEventsByPlace(placeId,count, isBad));
        }
        int total = 0;
        for (GenerationMessage message : messages) {
            total += message.getCount();
        }
        GenerationMessage msg = new GenerationMessage();
        msg.setType("generateEventsByPlaces");
        msg.setCount(total);
        msg.setMessage("Total Events Generated");
        messages.add(msg);
        return messages;
    }
    public GenerationMessage generateEventsByCity(String cityId, int count) throws Exception {
        boolean isBad;
        int m = random.nextInt(10);
        isBad = m <= 4;

        try {
            City city = cityService.getCityById(cityId);
            LOGGER.info(E.LEAF + " " + city.getCity() + " rating should be bad: "
                    + isBad + " - count: " + count);
            List<CityPlace> places = placesService.getPlacesByCity(cityId);
            userMap = new HashMap<>();
            int total = 0;
            for (int i = 0; i < count; i++) {
                int placeIndex = random.nextInt(places.size() - 1);
                CityPlace place = places.get(placeIndex);
                var isToBeExcluded =  false;
                for (String s : place.types) {
                    if (s.equalsIgnoreCase("school")
                            || s.equalsIgnoreCase("church")) {
                        isToBeExcluded = true;
                        LOGGER.info(E.YELLOW_STAR + "Ignore this place: " +
                                E.YELLOW_STAR + " " +
                                place.name);
                        break;
                    }
                }
                if (!isToBeExcluded) {
                    try {
                        total += generateEventAtPlace(place, getUnusedRandomUser(cityId), isBad);
                    } catch (Exception e) {
                        LOGGER.info(E.RED_DOT + E.RED_DOT + " " + e.getMessage());
                    }
                }

            }
            String x = "Events generated OK: " + total + " city: " + city.getCity();
            LOGGER.info(E.RED_APPLE + " " + x);
            GenerationMessage msg = new GenerationMessage();
            msg.setType("generateEventsByCity");
            msg.setCount(total);
            msg.setMessage(city.getCity() + ", " + city.getAdmin_name());
            return msg;
        } catch (Exception e) {
            LOGGER.info(E.RED_DOT +" generateEventsByCity: Error: " + e.getMessage());
            throw e;
        }
    }

    public GenerationMessage generateEventsByPlace(String placeId, int count, boolean isBad) throws Exception {
        CityPlace place = placesService.getPlaceById(placeId);
        int total = 0;
        userMap = new HashMap<>();
        for (int i = 0; i < count; i++) {
            try {
                total += generateEventAtPlace(place, getUnusedRandomUser(place.cityId), isBad);
                totalCount += total;
            } catch (Exception e) {
                LOGGER.info(E.RED_DOT + E.RED_DOT + " " + e.getMessage());
            }

        }
        String msg = " Place events generated: " + total + " at " + place.name;
        LOGGER.info(E.GREEN_APPLE + " " + msg);
        GenerationMessage message = new GenerationMessage();
        message.setCount(total);
        message.setMessage("Events generated for: " + place.name);
        message.setType("generateEventsByPlace");
        return message;

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
