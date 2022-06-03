package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class IncrementalProcessExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 读入股票数据流
        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"));

        // reduce的返回类型必须和输入类型相同
        // 为此我们将StockPrice拆成一个四元组 (股票代号，最大值、最小值，时间戳)
        DataStream<Tuple4<String, Double, Double, Long>> maxMin = stockStream
                .map(s -> Tuple4.of(s.symbol, s.price, s.price, 0L))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.LONG))
                .keyBy(s -> s.f0)
                .timeWindow(Time.seconds(10))
                .reduce(new MaxMinReduce(), new WindowEndProcessFunction());

        maxMin.print();

        env.execute("window aggregate function");
    }

    // 增量计算最大值和最小值
    public static class MaxMinReduce implements ReduceFunction<Tuple4<String, Double, Double, Long>> {
        @Override
        public Tuple4<String, Double, Double, Long> reduce(Tuple4<String, Double, Double, Long> a, Tuple4<String, Double, Double, Long> b) {
            return Tuple4.of(a.f0, Math.max(a.f1, b.f1), Math.min(a.f2, b.f2), 0L);
        }
    }

    // 利用ProcessFunction可以获取Context的特点，获取窗口结束时间
    public static class WindowEndProcessFunction extends ProcessWindowFunction<Tuple4<String, Double, Double, Long>, Tuple4<String, Double, Double, Long>, String, TimeWindow> {
        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple4<String, Double, Double, Long>> elements,
                            Collector<Tuple4<String, Double, Double, Long>> out) {
            long windowEndTs = context.window().getEnd();
            if (elements.iterator().hasNext()) {
                Tuple4<String, Double, Double, Long> firstElement = elements.iterator().next();
                out.collect(Tuple4.of(key, firstElement.f1, firstElement.f2, windowEndTs));
            }
        }
    }
}
