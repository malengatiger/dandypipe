package com.boha.datadriver.services;

import com.boha.datadriver.models.*;
import com.boha.datadriver.util.DB;
import com.boha.datadriver.util.E;
import com.boha.datadriver.util.SecretMgr;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import com.google.firebase.cloud.FirestoreClient;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.squareup.okhttp.*;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.logging.Logger;

/**
 * Manages the CityPlace resource. Creates CityPlaces on Firestore using the Places API
 */
@Service
public class PlacesService {
    private static final String prefix =
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json?";
    private static final Logger LOGGER = Logger.getLogger(PlacesService.class.getSimpleName());
    private static final Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();
    @Autowired
    private SecretMgr secretMgr;
    String buildLink(double lat, double lng, int radiusInMetres) throws Exception {
        StringBuilder sb = new StringBuilder();
        String placesAPIKey = secretMgr.getPlacesAPIKey();
        sb.append(prefix);
        sb.append("location=").append(lat).append(",").append(lng);
        sb.append("&radius=").append(radiusInMetres);
        sb.append("&key=").append(placesAPIKey);
        return sb.toString();
    }
    OkHttpClient client = new OkHttpClient();
    int MAX_PAGE_COUNT = 3;
    int pageCount;
    int totalPlaceCount = 0;
    void getCityPlaces(City city, int radiusInMetres, String pageToken) throws Exception {
        if (pageToken == null) {
            if (city.getCity().contains("Durban")
                    || city.getCity().contains("Pretoria")
                    || city.getCity().contains("Cape Town")
                    || city.getCity().contains("Johannesburg" )
                    || city.getCity().contains("Sandton" )
                    || city.getCity().contains("Bloemfontein")
                    || city.getCity().contains("Fourways" )) {
                MAX_PAGE_COUNT = 4;
                LOGGER.info(E.RED_DOT+E.RED_DOT+E.RED_DOT +
                        " MAX_PAGE_COUNT = 4 !!! Yay! " + city.getCity());
            } else {
                MAX_PAGE_COUNT =  2;
            }
        }
        String link = buildLink(city.getLatitude(),city.getLongitude(),radiusInMetres);
        if (pageToken != null) {
            link += "&pagetoken="+pageToken;
        }
        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR+ " " + link);
        HttpUrl.Builder urlBuilder
                = HttpUrl.parse(link).newBuilder();

        String url = urlBuilder.build().toString();

        Request request = new Request.Builder()
                .url(url)
                .build();
        Call call = client.newCall(request);
        Response response = call.execute();
        String mResp = response.body().string();
        Root root = GSON.fromJson(mResp, Root.class);
        for (CityPlace cityPlace : root.getResults()) {
            cityPlace.setCityId(city.getId());
            cityPlace.setCityName(city.getCity());
            cityPlace.setProvince(city.getAdminMame());
            cityPlace.setLatitude(cityPlace.getGeometry().getLocation().lat);
            cityPlace.setLongitude(cityPlace.getGeometry().getLocation().lng);
        }
        addCityPlacesToFirestore(root);
        pageCount++;
        totalPlaceCount += root.getResults().size();
        if (pageCount < MAX_PAGE_COUNT) {
            if (root.getNextPageToken() != null) {
                getCityPlaces(city, radiusInMetres, root.getNextPageToken());
            }
        }
    }

    void addCityPlacesToFirestore(Root root) throws Exception {
        Firestore c = FirestoreClient.getFirestore();
        for (CityPlace cityPlace : root.getResults()) {
            if (cityPlace.getGeometry() == null) {
                LOGGER.info(E.RED_DOT + E.RED_DOT + " geometry is null. Ignoring Firestore add!");
            } else {
                ApiFuture<DocumentReference> future = c.collection(DB.places).add(cityPlace);
                DocumentReference ref = future.get();
                LOGGER.info(E.RED_APPLE + E.RED_APPLE +
                        " " + cityPlace.getName() + " " + E.YELLOW_STAR + " path: " + ref.getPath());
            }
        }
    }
    @Autowired
    private CityService cityService;

    public List<CityPlace> getPlacesByCity(String cityId) throws Exception {
        List<CityPlace> cityPlaces = new ArrayList<>();
        Firestore c = FirestoreClient.getFirestore();
        City city = cityService.getCityById(cityId);
        ApiFuture<QuerySnapshot> future = c.collection("cityPlaces")
                .whereEqualTo("cityId", cityId)
                .get();
        QuerySnapshot snapshot = future.get();
        List<QueryDocumentSnapshot> list = snapshot.getDocuments();

        for (QueryDocumentSnapshot queryDocumentSnapshot : list) {
            CityPlace cityPlace = queryDocumentSnapshot.toObject(CityPlace.class);
            cityPlaces.add(cityPlace);
        }

        return cityPlaces;
    }

    public PlaceAggregate getPlaceAggregate(String placeId, int minutes) throws Exception {
        List<PlaceAggregate> cityPlaces = new ArrayList<>();
        Firestore c = FirestoreClient.getFirestore();
        DateTime dt = DateTime.now().minusMinutes(minutes);
        long m = dt.getMillis();
        var ref = c.collection(DB.events)
                .whereEqualTo("placeId",placeId)
                .whereGreaterThanOrEqualTo("longDate", m)
                .orderBy("longDate", Query.Direction.DESCENDING)
                .get().get();

        List<FlatEvent> events = new ArrayList<>();
        for (QueryDocumentSnapshot doc : ref.getDocuments()) {
            events.add(doc.toObject(FlatEvent.class));
        }

        LOGGER.info(E.RED_APPLE
                +" Events of last " + minutes + " minutes found: " + events.size());
        double totalAmount = 0.0;
        int totalRating = 0;
        double avgRating = 0.0;

        for (FlatEvent e : events) {
            totalAmount += e.getAmount();
            totalRating += e.getRating();
        }
        avgRating = Double.parseDouble("" + (totalRating/events.size()));
        PlaceAggregate agg = new PlaceAggregate();
        FlatEvent fe = events.get(0);

        agg.setAverageRating(avgRating);
        agg.setCityName(fe.getCityName());
        agg.setCityId(fe.getCityId());
        agg.setDate(DateTime.now().toDateTimeISO().toString());
        agg.setLatitude(fe.getLatitude());
        agg.setLongitude(fe.getLongitude());
        agg.setPlaceName(fe.getPlaceName());
        agg.setPlaceId(fe.getPlaceId());
        agg.setMinutes(minutes);
        agg.setLongDate(DateTime.now().getMillis());
        agg.setTotalSpent(totalAmount);
        agg.setNumberOfEvents(events.size());


        LOGGER.info(E.RED_APPLE+
                " Place Aggregate calculated: " + GSON.toJson(agg));
        return agg;
    }

    public CityPlace getPlaceById(String placeId) throws Exception {
        Firestore c = FirestoreClient.getFirestore();
        ApiFuture<QuerySnapshot> future = c
                .collection("cityPlaces")
                .whereEqualTo("place_id", placeId)
                .get();
        QuerySnapshot snapshot = future.get();
        List<QueryDocumentSnapshot> list = snapshot.getDocuments();

        CityPlace place = null;

        for (QueryDocumentSnapshot queryDocumentSnapshot : list) {
            place = queryDocumentSnapshot.toObject(CityPlace.class);

        }

//        LOGGER.info(E.RED_DOT  +
//                " " + city.getCity() + " has " + cityPlaces.size() + " places on file" );
        return place;
    }

    public String loadCityPlaces() throws Exception {
        List<City> cities = cityService.getCities();
        if (!cities.isEmpty()) {
            for (City city : cities) {
                LOGGER.info(E.BLUE_DOT + E.BLUE_DOT + E.BLUE_DOT +
                        " ... Finding places for " + city.getCity());
                pageCount = 0;
                getCityPlaces(city, 10000, null);

            }
        }

        return totalPlaceCount + " Total City Places Loaded " + E.AMP+E.AMP+E.AMP;
    }

    public long countPlaces() throws Exception {
        Firestore firestore = FirestoreClient.getFirestore();
        AggregateQuerySnapshot snapshot = firestore.collection("cityPlaces")
                .count()
                .get().get();
        long count = snapshot.getCount();
        LOGGER.info(E.AMP+E.AMP+E.AMP + " Counted " + count + " places");
        return count;
    }
}
