package com.boha.datadriver.services;

public class GenerationMessage {
    String type, message;
    int count;

    public GenerationMessage() {
    }

    public GenerationMessage(String type, String message, int count) {
        this.type = type;
        this.message = message;
        this.count = count;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
