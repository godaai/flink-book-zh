package com.flink.tutorials.java.api.types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// NOT POJO
public class StockPrice1 {

    // LOGGER 无getter和setter
    private Logger LOGGER = LoggerFactory.getLogger(StockPrice1.class);

    public String symbol;
    public Long timestamp;
    public Double price;

    public StockPrice1() {}
    public StockPrice1(String symbol, Long timestamp, Double price){
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.price = price;
    }
}