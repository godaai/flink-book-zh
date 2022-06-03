package com.flink.tutorials.java.chapter4_api.types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// NOT POJO
public class StockPriceNoGeterSeter {

    // LOGGER 无getter和setter
    private Logger LOGGER = LoggerFactory.getLogger(StockPriceNoGeterSeter.class);

    public String symbol;
    public double price;
    public long ts;

    public StockPriceNoGeterSeter() {}

    public StockPriceNoGeterSeter(String symbol, long timestamp, Double price){
        this.symbol = symbol;
        this.ts = timestamp;
        this.price = price;
    }
}