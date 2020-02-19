package com.flink.tutorials.java.api.utils.random;

public class RandomId {

    public String id;
    public long ts;

    public RandomId() {}

    public RandomId(String id, long ts) {
        this.id = id;
        this.ts = ts;
    }

    public static RandomId of(String id, long ts) {
        return new RandomId(id, ts);
    }

    public static RandomId of(Integer id, long ts) {
        return new RandomId(id.toString(), ts);
    }
}
