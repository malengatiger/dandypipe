package com.boha.datadriver.models;

import java.util.List;

public class GenerationResultsBag {
    private List<GenerationMessage> messages;
    private List<CityAggregate> aggregates;
    private DashboardData dashboardData;
    private String date;
    private double elapsedSeconds;

    public double getElapsedSeconds() {
        return elapsedSeconds;
    }

    public void setElapsedSeconds(double elapsedSeconds) {
        this.elapsedSeconds = elapsedSeconds;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public List<GenerationMessage> getMessages() {
        return messages;
    }

    public void setMessages(List<GenerationMessage> messages) {
        this.messages = messages;
    }

    public List<CityAggregate> getAggregates() {
        return aggregates;
    }

    public void setAggregates(List<CityAggregate> aggregates) {
        this.aggregates = aggregates;
    }

    public DashboardData getDashboardData() {
        return dashboardData;
    }

    public void setDashboardData(DashboardData dashboardData) {
        this.dashboardData = dashboardData;
    }
}
