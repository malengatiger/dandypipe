package com.boha.datadriver.services;

import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.models.GCSBlob;
import com.boha.datadriver.util.E;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

@Service
public class StorageService {
    private static final Logger LOGGER = Logger.getLogger(StorageService.class.getSimpleName());

    public StorageService() {
        LOGGER.info(E.RED_APPLE + E.RED_APPLE +
                "StorageService constructed: ");
    }

    private Storage storage;
    @Value("${bucketName}")

    private String bucketName;
    @Value("${projectId}")
    private String projectId;

    public String downloadObject(
            String objectName) {

        Storage storage = StorageOptions.newBuilder()
                .setProjectId(projectId)
                .build()
                .getService();
        byte[] content = storage.readAllBytes(bucketName, objectName);
        LOGGER.info(E.PEAR+" Downloaded object from GCS: " + objectName);
        return new String(content, StandardCharsets.UTF_8);
    }

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    public List<FlatEvent> getRecentFlatEvents(int hours) throws Exception {
        List<GCSBlob> list = listObjects(hours);
        List<FlatEvent> flatEvents = new ArrayList<>();
        Type listType = new TypeToken<ArrayList<FlatEvent>>(){}.getType();

        int index = 0;
        for (GCSBlob blob : list) {
            String content = downloadObject(blob.getName());
            String m =content.replace("}","},");
            String json = "[\n" + m + "\n]";
            //LOGGER.info(E.ORANGE_HEART+E.ORANGE_HEART+" " + json);
            try {
                List<FlatEvent> flats = GSON.fromJson(json, listType);
                flatEvents.addAll(flats);
                LOGGER.info(E.ORANGE_HEART
                        + new DateTime(blob.getCreateTime()) + " "
                        + blob.getName() + " events: " + E.RED_DOT + " " + flats.size());
            } catch (Exception e) {
                LOGGER.info("Problem parsing list: " + e.getMessage());
            }

        }
        LOGGER.info(E.BLUE_HEART+E.BLUE_HEART+
                "All FlatEvents from GCS files: " + flatEvents.size());
        return flatEvents;
    }

    public List<GCSBlob> listObjects(int hours) throws Exception {
        LOGGER.info(E.AMP + E.AMP + E.AMP + " Starting to list bucket blobs: "
                + bucketName + " projectId: " + projectId);

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        Page<Blob> blobs = storage.list(bucketName);
        List<GCSBlob> list = new ArrayList<GCSBlob>();

        long now = DateTime.now().minusHours(hours).getMillis();
        for (Blob blob : blobs.iterateAll()) {
            if (blob.getName().contains("events/page")) {
                if (blob.getCreateTime() > now) {
                    GCSBlob g = new GCSBlob();
                    g.setCreateTime(new DateTime(blob.getCreateTime()).toDateTimeISO().toString());
                    g.setName(blob.getName());
                    g.setSize(blob.getSize().intValue());
                    list.add(g);

                }
            }

        }

        Collections.sort(list);
        return list;
    }
}
