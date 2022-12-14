package com.boha.datadriver.util;

import com.boha.datadriver.models.Event;
import com.boha.datadriver.models.FlatEvent;

import java.util.logging.Logger;

public class FlatEventGetter {
    private static final Logger LOGGER = Logger.getLogger(FlatEventGetter.class.getSimpleName());


    public static FlatEvent getFlatEvent(Event event) throws Exception {
        FlatEvent  flatEvent = new FlatEvent();
        flatEvent.setAmount(event.getAmount());
        flatEvent.setCityId(event.getCityPlace().getCityId());
        flatEvent.setCityName(event.getCityPlace().getCityName());
        flatEvent.setDate(event.getDate());
        flatEvent.setEventId(event.getEventId());
        flatEvent.setLatitude(event.getCityPlace().getGeometry().getLocation().lat);
        flatEvent.setLongDate(event.getLongDate());
        flatEvent.setLongitude(event.getCityPlace().getGeometry().getLocation().lng);
        flatEvent.setPlaceId(event.getCityPlace().getPlaceId());
        flatEvent.setPlaceName(event.getCityPlace().getName());
        flatEvent.setRating(event.getRating());
        if (event.getUser() !=  null) {
            flatEvent.setUserId(event.getUser().getUserId());
            flatEvent.setFirstName(event.getUser().getFirstName());
            flatEvent.setLastName(event.getUser().getLastName());
            flatEvent.setMiddleInitial(event.getUser().getMiddleInitial());
            flatEvent.setUserCityId(event.getUser().getCityId());
            flatEvent.setUserCityName(event.getUser().getCityName());
        } else {
            String msg = "User not available for event";
            LOGGER.info(E.RED_DOT+E.RED_DOT+ " " + msg);
            throw new RuntimeException(msg);
        }
        if (event.getCityPlace().getTypes() != null) {
            StringBuilder bf = new StringBuilder();
            int index = 0;
            for (String t: event.getCityPlace().getTypes()) {
                bf.append(t);
                if (index < event.getCityPlace().getTypes().size() - 1) {
                    bf.append(", ");
                }

                index++;
            }
            flatEvent.setTypes(bf.toString());
        }
        flatEvent.setVicinity(event.getCityPlace().getVicinity());
        return flatEvent;

    }

}
