package com.boha.datadriver.services;

import com.boha.datadriver.models.City;
import com.boha.datadriver.models.User;
import com.boha.datadriver.util.E;
import com.boha.datadriver.util.LogControl;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import com.google.firebase.cloud.FirestoreClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Manages the users needed for the demo.
 */
@Service
public class UserService {
    private static final Logger LOGGER = Logger.getLogger(UserService.class.getSimpleName());


    public UserService() {
        LOGGER.info(E.AMP + E.AMP + E.AMP + " UserService constructed");
    }

    @Autowired
    private LogControl logControl;
    public void addUser(User user) throws RuntimeException {
        Firestore c = FirestoreClient.getFirestore();
        try {
            c.collection("users").add(user);
        } catch (Exception e) {
            throw new RuntimeException("Failed to add user: "  +e.getMessage());
        }
    }
    public long countUsers() throws RuntimeException {
        Firestore c = FirestoreClient.getFirestore();
        long count = 0;
        try {
            AggregateQuery query = c.collection("users").count();
            count = query.get().get().getCount();
        } catch (Exception e) {
            throw new RuntimeException("Failed to count users: "  +e.getMessage());
        }
        return count;
    }

    @Autowired
    private CityService cityService;

    public List<User> getCityUsers(String cityId) throws Exception {
        Firestore firestore = FirestoreClient.getFirestore();
        City city = cityService.getCityById(cityId);

        QuerySnapshot snapshot = firestore
                .collection("users")
                .whereEqualTo("cityId", cityId)
                .get().get();

        List<QueryDocumentSnapshot> docs = snapshot.getDocuments();
        List<User> users = new ArrayList<>();
        for (QueryDocumentSnapshot doc : docs) {
            User user = doc.toObject(User.class);
            users.add(user);
        }
        if (city != null) {
            LOGGER.info(E.CHECK + E.CHECK + " Found " + users.size()
                    + " users registered in " + city.getCity());
        }
        return users;
    }

    public User getUserById(String userId) throws Exception {
        Firestore firestore = FirestoreClient.getFirestore();
        ApiFuture<QuerySnapshot> future = firestore.collection("users")
                .whereEqualTo("userId", userId)
                .get();
        QuerySnapshot snapshot = future.get();
        List<QueryDocumentSnapshot> docs = snapshot.getDocuments();
        User user = null;
        for (QueryDocumentSnapshot doc : docs) {
            user = doc.toObject(User.class);
        }
        if (user == null) {
            LOGGER.info(E.RED_DOT + E.RED_DOT + E.RED_DOT +
                    "User not found!");
        }
        return user;
    }

}
