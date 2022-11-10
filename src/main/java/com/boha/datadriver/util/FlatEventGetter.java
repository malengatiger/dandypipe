package com.boha.datadriver.util;

import com.boha.datadriver.models.Event;
import com.boha.datadriver.models.FlatEvent;

public class FlatEventGetter {
    public static FlatEvent getFlatEvent(Event event) {
        FlatEvent  flatEvent = new FlatEvent();
        flatEvent.setAmount(event.getAmount());
        flatEvent.setCityId(event.getCityPlace().cityId);
        flatEvent.setCityName(event.getCityPlace().cityName);
        flatEvent.setDate(event.getDate());
        flatEvent.setEventId(event.getEventId());
        flatEvent.setLatitude(event.getCityPlace().geometry.location.lat);
        flatEvent.setLongDate(event.getLongDate());
        flatEvent.setLongitude(event.getCityPlace().geometry.location.lng);
        flatEvent.setPlaceId(event.getCityPlace().place_id);
        flatEvent.setPlaceName(event.getCityPlace().name);
        flatEvent.setRating(event.getRating());
        if (event.getCityPlace().types != null) {
            StringBuilder bf = new StringBuilder();
            int index = 0;
            for (String t: event.getCityPlace().types) {
                bf.append(t);
                if (index < event.getCityPlace().types.size() - 1) {
                    bf.append(", ");
                }

                index++;
            }
            flatEvent.setTypes(bf.toString());
        }
        flatEvent.setVicinity(event.getCityPlace().vicinity);
        return flatEvent;

    }

}
