package com.flink.tutorials.java.utils.stock;

import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StockReaderFormat extends SimpleStreamFormat<StockPrice> {
    private static final long serialVersionUID = 1L;

    @Override
    public Reader<StockPrice> createReader(Configuration config, FSDataInputStream stream) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        return new Reader<>() {
            @Override
            public StockPrice read() throws IOException {
                String line = reader.readLine();
                if (line == null) {
                    return null;
                }

                String[] fields = line.split(",");
                if (fields.length < 5) {
                    System.err.println("Invalid line: " + line + " (expected at least 5 fields)");
                    return read(); // 跳过无效行，继续读取下一行
                }

                try {
                    String symbol = fields[0];
                    String date = fields[1];
                    String time = fields[2];
                    double price = Double.parseDouble(fields[3]);
                    int volume = Integer.parseInt(fields[4]);

                    long ts = parseDateTime(date, time);
                    String mediaStatus = "";

                    return new StockPrice(symbol, price, ts, volume, mediaStatus);
                } catch (NumberFormatException | ParseException e) {
                    System.err.println("Failed to parse line: " + line + " - " + e.getMessage());
                    return read(); // 解析失败时继续读取下一行
                }
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    private long parseDateTime(String date, String time) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        Date dt = sdf.parse(date + " " + time);
        return dt.getTime();
    }

    @Override
    public TypeInformation<StockPrice> getProducedType() {
        return TypeInformation.of(StockPrice.class);
    }
}