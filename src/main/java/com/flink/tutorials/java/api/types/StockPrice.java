package com.flink.tutorials.java.api.types;

public class StockPrice {
    public String symbol;
    public Long timestamp;
    public Double price;

    public StockPrice() {}
    public StockPrice(String symbol, Long timestamp, Double price){
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.price = price;
    }
}
