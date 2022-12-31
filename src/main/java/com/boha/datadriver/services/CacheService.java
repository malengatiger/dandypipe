package com.boha.datadriver.services;

import com.boha.datadriver.models.CacheBag;
import com.boha.datadriver.models.City;
import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.util.E;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
public class CacheService {
    private static final Logger LOGGER = Logger.getLogger(CacheService.class.getSimpleName());
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    final CityService cityService;
    final PlacesService placesService;
    final DashboardService dashboardService;
    final EventService eventService;
    final CityAggregateService cityAggregateService;

    public CacheService(CityService cityService, PlacesService placesService,
                        DashboardService dashboardService, EventService eventService,
                        CityAggregateService cityAggregateService) {
        this.cityService = cityService;
        this.placesService = placesService;
        this.dashboardService = dashboardService;
        this.eventService = eventService;
        this.cityAggregateService = cityAggregateService;
        LOGGER.info(E.AMP + " CacheService constructed and services injected");
    }

    public CacheBag getDataForCache(int minutesAgo) throws Exception {
        LOGGER.info(E.BLUE_HEART + E.BLUE_HEART + E.BLUE_HEART +
                " .... getDataForCache starting");

        long start = System.currentTimeMillis();
        CacheBag bag = new CacheBag();
        try {
            bag.setCities(cityService.getCities());
            bag.setPlaces(placesService.getPlaces());
            try {
                bag.setAggregates(cityAggregateService.getCityAggregates(minutesAgo));
            } catch (Exception e) {
                LOGGER.severe(E.RED_DOT + E.RED_DOT + " Error getting aggregates: " + e.getMessage());
            }
            try {
                bag.setDashboards(dashboardService.getDashboardData(minutesAgo));
            } catch (Exception e) {
                LOGGER.severe(E.RED_DOT + E.RED_DOT + " Error getting dashboards: " + e.getMessage());
            }

            bag.setDate(DateTime.now().toDateTimeISO().toString());
            long end = System.currentTimeMillis();
            double elapsed = Double.parseDouble("" + (end - start) / 1000 / 60);
            bag.setElapsedSeconds(elapsed);

        } catch (Exception e) {
            LOGGER.severe(E.RED_DOT + E.RED_DOT + E.RED_DOT + " cache query problem: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        return bag;
    }
    public List<FlatEvent> getCityEvents(String cityId, int minutesAgo) throws Exception {
        List<FlatEvent> list = eventService.getCityEvents(cityId,minutesAgo);
        LOGGER.info(E.AMP + " City events found, minutesAgo:  " + minutesAgo + " total events: " + list.size());
        return list;
    }
    public String getEventZippedFilePath(String cityId, int minutesAgo) throws Exception {
        long start = System.currentTimeMillis();
        List<FlatEvent>  events = eventService.getCityEvents(cityId, minutesAgo);

        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR
                +" Total Events: " + events.size() + " from "+minutesAgo+" minutesAgo");
        long end1 = System.currentTimeMillis();
        long elapsed1 = (end1 - start)/1000/60;

        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR
                +" Time taken to read events from Firestore: " + elapsed1 + " elapsed minutes ");

        File dir = new File("temporary");
        if (!dir.exists()) {
            boolean b = dir.mkdir();
        }

        String json = GSON.toJson(events);
        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR
                +" Size of events json string: " + json.length()
                + " " + events.size() + " events");

        File newFile = new File(dir + "/events-" + System.currentTimeMillis() + ".json");
        FileWriter fileWriter = new FileWriter(newFile);
        fileWriter.write(json);
        fileWriter.close();
        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR
                +" create zip file from: " + newFile.toURI().getPath());
        File zipped = zipSingleFile(Paths.get(newFile.toURI()));

        long end2 = System.currentTimeMillis();
        long elapsed2 = (end2 - start)/1000/60;

        LOGGER.info(E.YELLOW_STAR+E.YELLOW_STAR
                +" Zipped files created: " + zipped.length() + " bytes, elapsed: " + elapsed2 + " minutes");

        return zipped.getPath();
    }
    private File zipSingleFile(Path source)
            throws Exception {
        File dir = new File("temporary");
        if (!dir.exists()) {
            dir.mkdir();
        }
        File file = new File(dir + "/events-" + System.currentTimeMillis() + ".zip");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        try (
                ZipOutputStream zos = new ZipOutputStream(fileOutputStream);
                FileInputStream fis = new FileInputStream(source.toFile());
        ) {

            ZipEntry zipEntry = new ZipEntry(source.getFileName().toString());
            zos.putNextEntry(zipEntry);
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                zos.write(buffer, 0, len);
            }
            zos.closeEntry();
        }
        LOGGER.info(E.BLUE_HEART + " Zip file created: " + file.length() + " name: " + file.getName());

        return file;

    }
}
