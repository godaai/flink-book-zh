package com.flink.tutorials.java.api.time;

import com.flink.tutorials.java.utils.stock.Media;
import com.flink.tutorials.java.utils.stock.MediaSource;
import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

public class KeyCoProcessFunctonExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读入股票数据流
        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<StockPrice>() {
                    @Override
                    public long extractAscendingTimestamp(StockPrice stockPrice) {
                        return stockPrice.ts;
                    }
                });

        // 读入媒体评价数据流
        DataStream<Media> mediaStream = env
                .addSource(new MediaSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Media>() {
                    @Override
                    public long extractAscendingTimestamp(Media media) {
                        return media.ts;
                    }
                });

        DataStream<StockPrice> joinStream = stockStream.connect(mediaStream)
                .keyBy("symbol", "symbol")
                // 调用process函数
                .process(new JoinStockMediaProcessFunction());
        joinStream.print();

        env.execute("coprocess function");
    }

    /**
     * 四个泛型：Key，第一个流类型，第二个流类型，输出。
      */
    public static class JoinStockMediaProcessFunction extends KeyedCoProcessFunction<String, StockPrice, Media, StockPrice> {
        // mediaState
        private ValueState<String> mediaState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从RuntimeContext中获取状态
            mediaState = getRuntimeContext().getState(
                    new ValueStateDescriptor<String>("mediaStatusState", Types.STRING));
        }

        @Override
        public void processElement1(StockPrice stock, Context context, Collector<StockPrice> collector) throws Exception {
            String mediaStatus = mediaState.value();
            if (null != mediaStatus) {
                stock.mediaStatus = mediaStatus;
                collector.collect(stock);
            }
        }

        @Override
        public void processElement2(Media media, Context context, Collector<StockPrice> collector) throws Exception {
            // 第二个流更新mediaState
            mediaState.update(media.status);
        }
    }
}
