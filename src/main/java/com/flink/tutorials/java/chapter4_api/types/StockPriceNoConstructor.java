package com.flink.tutorials.java.chapter4_api.types;

// NOT POJO
public class StockPriceNoConstructor {

    public String symbol;
    public double price;
    public long ts;

    // 缺少无参数构造函数

    public StockPriceNoConstructor(String symbol, Long timestamp, Double price){
        this.symbol = symbol;
        this.ts = timestamp;
        this.price = price;
    }
}
