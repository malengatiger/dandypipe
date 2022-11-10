package com.boha.datadriver.util;

import com.boha.datadriver.services.Generator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.logging.Logger;

@Component
public class LogControl {
    static final Logger LOGGER = Logger.getLogger(Generator.class.getSimpleName());
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    @Value("${spring.profiles.active}")
    private String profile;

    public void info(String log) {
        if (profile.equalsIgnoreCase("dev")) {
            LOGGER.info(log);;
        }
    }
}
