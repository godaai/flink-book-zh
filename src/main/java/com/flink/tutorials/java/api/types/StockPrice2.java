package com.flink.tutorials.java.api.types;

// NOT POJO
public class StockPrice2 {

    public String symbol;
    public Long timestamp;
    public Double price;

    // 缺少无参数构造函数

    public StockPrice2(String symbol, Long timestamp, Double price){
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.price = price;
    }
}
