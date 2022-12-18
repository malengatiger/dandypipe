package com.boha.datadriver.services;

import com.boha.datadriver.models.CacheBag;
import com.boha.datadriver.util.E;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    public CacheBag getZipFileForCache(int minutesAgo) throws Exception {
        LOGGER.info(E.BLUE_HEART + E.BLUE_HEART +E.BLUE_HEART +
                " .... getZipFileForCache starting");

        long start = System.currentTimeMillis();

        CacheBag bag = new CacheBag();
        bag.setCities(cityService.getCities());
        bag.setPlaces(placesService.getPlaces());
        bag.setAggregates(cityAggregateService.getCityAggregates(minutesAgo));
        bag.setDashboards(dashboardService.getDashboardData(minutesAgo));
        bag.setEvents(eventService.getRecentEvents(minutesAgo));
        bag.setDate(DateTime.now().toDateTimeISO().toString());

        long end = System.currentTimeMillis();
        double elapsed = Double.parseDouble("" + (end-start)/1000/60);
        bag.setElapsedSeconds(elapsed);

        String bagJson = GSON.toJson(bag);
        LOGGER.info(E.BLUE_HEART +" Total length of bagJson: "
                + bagJson.length() + "");

        return bag;
    }

    private File zipSingleFile(Path source)
            throws Exception {
        File file = new File("" + System.currentTimeMillis() + ".zip");
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
        LOGGER.info(E.BLUE_HEART+" Zip file created: " + file.length() + " name: " + file.getName());
        return file;

    }
}
