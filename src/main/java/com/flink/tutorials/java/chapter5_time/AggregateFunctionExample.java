package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AggregateFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 读入股票数据流
        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"));

        DataStream<Tuple2<String, Double>> average = stockStream
                .keyBy(s -> s.symbol)
                .timeWindow(Time.seconds(10))
                .aggregate(new AverageAggregate());

        average.print();

        env.execute("window aggregate function");
    }

    /**
     * 接收三个泛型：
     * IN: StockPrice
     * ACC：(String, Double, Int) - (symbol, sum, count)
     * OUT: (String, Double) - (symbol, average)
     */
    public static class AverageAggregate implements AggregateFunction<StockPrice, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return Tuple3.of("", 0d, 0);
        }

        @Override
        public Tuple3<String, Double, Integer> add(StockPrice item, Tuple3<String, Double, Integer> accumulator) {
            double price = accumulator.f1 + item.price;
            int count = accumulator.f2 + 1;
            return Tuple3.of(item.symbol, price, count);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
            return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
