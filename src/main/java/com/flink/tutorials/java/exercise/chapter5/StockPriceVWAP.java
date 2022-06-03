package com.flink.tutorials.java.exercise.chapter5;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * This demo shows how to calculate Volume-Weighted Average Price (VWAP)
 * of a stock data streams. The time window is 5 minutes.
 * See https://www.investopedia.com/terms/v/vwap.asp for VMAP
 */

public class StockPriceVWAP {

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

        DataStream<Tuple2<String, Double>> stream = inputStream
                .keyBy(stockPrice -> stockPrice.symbol)
                .timeWindow(Time.seconds(5))
                .process(new VWAPProcessFunction());

        stream.print();

        env.execute("stock price");
    }

    /**
     * Four Generic Types in the Function
     * IN: Input Data Type - StockPrice
     * OUT: Output Data Type - (String, Double, Double, Double, Double)
     * KEY：Key - String
     * W: Window - TimeWindow
     */
    public static class VWAPProcessFunction extends ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<StockPrice> elements, Collector<Tuple2<String ,Double>> out) {
            String symbol = key;
            Double volumeSum = 0.0d;
            Double priceVolumeSum = 0.0d;

            for (StockPrice element: elements) {
                priceVolumeSum += element.price * element.volume;
                volumeSum += element.volume;
            }

            out.collect(Tuple2.of(symbol, priceVolumeSum / volumeSum));
        }
    }
}
