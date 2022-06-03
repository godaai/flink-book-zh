package com.flink.tutorials.java.exercise.chapter5;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * This demo shows how to calculate OHLC (Open/High/Low/Close)
 * of a stock data streams. The time window is 5 minutes.
 */

public class StockPriceOHLC {

    public static void main(String[] args) throws Exception {

        // get Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        // read data stream
        DataStream<StockPrice> inputStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                );

        DataStream<Tuple5<String, Double, Double, Double, Double>> ohlcStream = inputStream
                .keyBy(stockPrice -> stockPrice.symbol)
                .timeWindow(Time.minutes(5))
                .process(new OHLCProcessFunction());

        ohlcStream.print();

        env.execute("stock price");
    }

    /**
     * Four Generic Types in the Function
     * IN: Input Data Type - StockPrice
     * OUT: Output Data Type (String, Double, Double, Double, Double)
     * KEY：Key - String
     * W: Window - TimeWindow
     */
    public static class OHLCProcessFunction extends ProcessWindowFunction<StockPrice, Tuple5<String, Double, Double, Double, Double>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<StockPrice> elements, Collector<Tuple5<String ,Double, Double, Double, Double>> out) {
            String symbol = key;

            Double open = elements.iterator().next().price;
            Double high = open;
            Double low = open;
            Double close = open;


            for (StockPrice element: elements) {
                if (high < element.price) {
                    high = element.price;
                }
                if (low > element.price) {
                    low = element.price;
                }
                close = element.price;
            }

            out.collect(Tuple5.of(symbol, open, high, low, close));
        }
    }
}
