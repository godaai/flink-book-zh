package com.flink.tutorials.java.utils.stock;

public class Media {
    public String symbol;
    public long ts;
    public String status;

    public Media() {}

    public Media(String symbol, long ts, String status) {
        this.symbol = symbol;
        this.ts = ts;
        this.status = status;
    }
}
