package com.boha.datadriver.util;

import com.boha.datadriver.DataDriverApplication;
import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload.JsonPayload;
import com.google.cloud.logging.Severity;
import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class WriteLogEntry {
    private static final Logger LOGGER = Logger.getLogger(WriteLogEntry.class.getSimpleName());

    @Value("${logName}")
    private String logName;
    public void info(String log) {
        try (Logging logging = LoggingOptions.getDefaultInstance().getService()) {
            Map<String, String> payload =
                    ImmutableMap.of(
                            "log", log);
            LogEntry entry =
                    LogEntry.newBuilder(JsonPayload.of(payload))
                            .setSeverity(Severity.INFO)
                            .setLogName(logName)
                            .setResource(MonitoredResource.newBuilder("global").build())
                            .build();
            // Writes the log entry asynchronously
            logging.write(Collections.singleton(entry));
            // Optional - flush any pending log entries just before Logging is closed
            logging.flush();
//            LOGGER.info(E.RED_DOT + " WriteLogEntry is working????");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
