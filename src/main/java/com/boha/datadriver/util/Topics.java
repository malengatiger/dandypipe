package com.boha.datadriver.util;

import com.boha.datadriver.services.EventPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Component
public class Topics {
    static final Logger LOGGER = Logger.getLogger(EventPublisher.class.getSimpleName());
    @Value("${eventTopicId}")
    private String eventTopicId;
    @Value("${flatTopicId}")
    private String flatTopicId;
    @Value("${bigQueryTopicId}")
    private String bigQueryTopicId;
    @Value("${pullTopicId}")
    private String pullTopicId;

    public List<String> printTopicNames() {
        LOGGER.info(E.RED_APPLE+E.RED_APPLE+
                "PubSub Topic: " + eventTopicId);
        LOGGER.info(E.RED_APPLE+E.RED_APPLE+
                "PubSub Topic: " + flatTopicId);
        LOGGER.info(E.RED_APPLE+E.RED_APPLE+
                "PubSub Topic: " + bigQueryTopicId);
        LOGGER.info(E.RED_APPLE+E.RED_APPLE+
                "PubSub Topic: " + pullTopicId);
        List<String>  list = new ArrayList<>();
        list.add(eventTopicId);
        list.add(flatTopicId);
        list.add(bigQueryTopicId);
        list.add(pullTopicId);
        return list;

    }
}
