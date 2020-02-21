package com.flink.tutorials.java.utils.stock;

/**
 * POJO StockPrice
 * symbol      股票代号
 * ts          时间戳
 * price       价格
 * volume      交易量
 * mediaStatus 媒体对该股票的评价状态
 * */

public class StockPrice {
    public String symbol;
    public double price;
    public long ts;
    public int volume;
    public String mediaStatus;

    public StockPrice() {}

    public StockPrice(String symbol, double price, long ts, int volume, String mediaStatus){
        this.symbol = symbol;
        this.price = price;
        this.ts = ts;
        this.volume = volume;
        this.mediaStatus = mediaStatus;
    }

    public static StockPrice of(String symbol, double price, long ts, int volume) {
        return new StockPrice(symbol, price, ts, volume, "");
    }

    @Override
    public String toString() {
        return "(" + this.symbol + "," +
                this.price + "," + this.ts +
                "," + this.volume + "," +
                this.mediaStatus + ")";
    }
}
