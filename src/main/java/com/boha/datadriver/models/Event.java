package com.boha.datadriver.models;

public class Event {
    private String eventId;
    private String date;
    private CityPlace cityPlace;
    private double amount;
    private int rating;
    private long longDate;


    public long getLongDate() {
        return longDate;
    }

    public void setLongDate(long longDate) {
        this.longDate = longDate;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public CityPlace getCityPlace() {
        return cityPlace;
    }

    public void setCityPlace(CityPlace cityPlace) {
        this.cityPlace = cityPlace;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }
    
    public FlatEvent getFlatEvent() {
        FlatEvent  f = new FlatEvent();
        f.setAmount(this.getAmount());
        f.setCityId(this.getCityPlace().cityId);
        f.setCityName(this.getCityPlace().cityName);
        f.setDate(this.getDate());
        f.setEventId(this.getEventId());
        f.setLatitude(this.getCityPlace().geometry.location.lat);
        f.setLongDate(this.getLongDate());
        f.setLongitude(this.getCityPlace().geometry.location.lng);
        f.setPlaceId(this.getCityPlace().place_id);
        f.setPlaceName(this.getCityPlace().name);
        f.setRating(this.getRating());
        if (this.getCityPlace().types != null) {
            f.setTypes(this.getCityPlace().types);
        }
        f.setVicinity(this.getCityPlace().vicinity);
        return f;

    }
}
