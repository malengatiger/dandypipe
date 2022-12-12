package com.boha.datadriver.models;

import com.google.gson.annotations.SerializedName;

public class City {
    private String city, lat,lng,country,iso2,capital;
    @SerializedName("admin_name")
    private String adminMame;
    @SerializedName("population_proper")
    private String populationProper;
    double latitude,longitude;
    int pop;
    String id;

    public String getAdminMame() {
        return adminMame;
    }

    public void setAdminMame(String adminMame) {
        this.adminMame = adminMame;
    }

    public String getPopulationProper() {
        return populationProper;
    }

    public void setPopulationProper(String populationProper) {
        this.populationProper = populationProper;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLng() {
        return lng;
    }

    public void setLng(String lng) {
        this.lng = lng;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getIso2() {
        return iso2;
    }

    public void setIso2(String iso2) {
        this.iso2 = iso2;
    }


    public String getCapital() {
        return capital;
    }

    public void setCapital(String capital) {
        this.capital = capital;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public int getPop() {
        return pop;
    }

    public void setPop(int pop) {
        this.pop = pop;
    }
}
