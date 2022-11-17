package com.boha.datadriver.models;

public class GenerationMessage implements  Comparable<GenerationMessage>{
    String type, message;
    int count;

    public GenerationMessage() {
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

    @Override
    public int compareTo(GenerationMessage generationMessage) {
        return this.message.compareToIgnoreCase(generationMessage.getMessage());
    }
}
