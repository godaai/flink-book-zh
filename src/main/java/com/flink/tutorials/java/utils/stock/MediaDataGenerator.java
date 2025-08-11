package com.flink.tutorials.java.utils.stock;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MediaDataGenerator {
    private static final List<String> SYMBOLS = Arrays.asList("US2.AAPL", "US1.AMZN", "US1.BABA");
    private static final long START_TS = 1578447000000L; // 2020/1/8 9:30:0

    public static void generateMediaData(String filePath, int numRecords) throws IOException {
        Random rand = new Random();
        
        try (FileWriter writer = new FileWriter(filePath)) {
            for (int i = 0; i < numRecords; i++) {
                for (String symbol : SYMBOLS) {
                    String status = rand.nextGaussian() > 0.05 ? "POSITIVE" : "NORMAL";
                    long timestamp = START_TS + i * 1000;
                    writer.write(String.format("%s,%d,%s%n", symbol, timestamp, status));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        // 生成测试数据文件
        generateMediaData("media-data.csv", 100);
    }
}
