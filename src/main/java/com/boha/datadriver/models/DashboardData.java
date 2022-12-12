package com.boha.datadriver.models;

public class DashboardData {
    private long events, cities, places, users;
    private double amount, averageRating;
    private int minutesAgo;
    private long longDate;
    private String date;

    public long getLongDate() {
        return longDate;
    }

    public void setLongDate(long longDate) {
        this.longDate = longDate;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public long getEvents() {
        return events;
    }

    public void setEvents(long events) {
        this.events = events;
    }

    public long getCities() {
        return cities;
    }

    public void setCities(long cities) {
        this.cities = cities;
    }

    public long getPlaces() {
        return places;
    }

    public void setPlaces(long places) {
        this.places = places;
    }

    public long getUsers() {
        return users;
    }

    public void setUsers(long users) {
        this.users = users;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public double getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(double averageRating) {
        this.averageRating = averageRating;
    }

    public int getMinutesAgo() {
        return minutesAgo;
    }

    public void setMinutesAgo(int minutesAgo) {
        this.minutesAgo = minutesAgo;
    }
}
