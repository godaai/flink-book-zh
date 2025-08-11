package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockReaderFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessWindowFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读入股票数据流
        String filePath = ClassLoader.getSystemResource("stock/stock-tick-20200108.csv")
                .getPath();
        FileSource<StockPrice> source = FileSource
                .forRecordStreamFormat(new StockReaderFormat(), new Path(filePath))
                .build();
        DataStream<StockPrice> stockStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "StockSource");

        DataStream<Tuple2<String, Double>> frequency = stockStream
                .keyBy(s -> s.symbol)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .process(new FrequencyProcessFunction());

        frequency.print();

        env.execute("window aggregate function");
    }

    public static class FrequencyProcessFunction extends ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<StockPrice> elements, Collector<Tuple2<String, Double>> out) {

            Map<Double, Integer> countMap = new HashMap<>();

            elements.forEach(element ->
                    countMap.compute(element.price, (k, v) -> (v == null) ? 1 : v + 1)
            );

            List<Map.Entry<Double, Integer>> sortedEntries = countMap.entrySet().stream()
                    .sorted(Map.Entry.<Double, Integer>comparingByValue().reversed())
                    .collect(Collectors.toList());

            if (!sortedEntries.isEmpty()) {
                out.collect(Tuple2.of(key, sortedEntries.get(0).getKey()));
            }
        }
    }
}