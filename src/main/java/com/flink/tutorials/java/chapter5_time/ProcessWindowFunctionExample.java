package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class ProcessWindowFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 读入股票数据流
        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"));

        DataStream<Tuple2<String, Double>> frequency = stockStream
                .keyBy(s -> s.symbol)
                .timeWindow(Time.seconds(10))
                .process(new FrequencyProcessFunction());

        frequency.print();

        env.execute("window aggregate function");
    }

    /**
     * 接收四个泛型：
     * IN: 输入类型 StockPrice
     * OUT: 输出类型 (String, Double)
     * KEY：Key String
     * W: 窗口 TimeWindow
     */
    public static class FrequencyProcessFunction extends ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<StockPrice> elements, Collector<Tuple2<String, Double>> out) {

            Map<Double, Integer> countMap = new HashMap<>();

            for (StockPrice element: elements) {
                if (countMap.containsKey(element.price)) {
                    int count = countMap.get(element.price);
                    countMap.put(element.price, count + 1);
                } else {
                    countMap.put(element.price, 1);
                }
            }

            // 按照出现次数从高到低排序
            List<Map.Entry<Double, Integer>> list = new LinkedList<>(countMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<Double, Integer>> (){
                public int compare(Map.Entry<Double, Integer> o1, Map.Entry<Double, Integer> o2) {
                    if (o1.getValue() < o2.getValue()) {
                        return 1;
                    }
                    else {
                        return -1;
                    }
                }
            });

            // 选出出现次数最高的输出到Collector
            if (list.size() > 0) {
                out.collect(Tuple2.of(key, list.get(0).getKey()));
            }
        }
    }
}
