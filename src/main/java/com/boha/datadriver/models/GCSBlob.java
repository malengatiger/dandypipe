package com.boha.datadriver.models;

import com.google.firebase.database.annotations.NotNull;

public class GCSBlob implements Comparable<GCSBlob>{
    String createTime, name;
    int size;

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }


    @Override
    public int compareTo(GCSBlob gcsBlob) {

        return this.createTime.compareTo(gcsBlob.createTime);
    }
}
