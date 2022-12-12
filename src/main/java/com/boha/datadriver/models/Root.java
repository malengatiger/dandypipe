package com.boha.datadriver.models;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;

public class Root{
    @SerializedName("html_attributions")
    private ArrayList<String> htmlAttributions;
    @SerializedName("next_page_token")
    private String nextPageToken;
    private ArrayList<CityPlace> results;
    private String status;

    public ArrayList<String> getHtmlAttributions() {
        return htmlAttributions;
    }

    public void setHtmlAttributions(ArrayList<String> htmlAttributions) {
        this.htmlAttributions = htmlAttributions;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }

    public void setNextPageToken(String nextPageToken) {
        this.nextPageToken = nextPageToken;
    }

    public ArrayList<CityPlace> getResults() {
        return results;
    }

    public void setResults(ArrayList<CityPlace> results) {
        this.results = results;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
