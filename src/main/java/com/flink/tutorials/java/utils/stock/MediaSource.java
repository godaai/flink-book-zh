package com.flink.tutorials.java.utils.stock;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MediaSource extends RichSourceFunction<Media> {

    // Source是否正在运行
    private boolean isRunning = true;
    // 起始timestamp 2020/1/8 9:30:0 与数据集中的起始时间相对应
    private long startTs = 1578447000000L;
    private Random rand = new Random();
    private List<String> symbolList = Arrays.asList("US2.AAPL", "US1.AMZN", "US1.BABA");

    @Override
    public void run(SourceContext<Media> srcCtx) throws Exception {
        long inc = 0;
        while (isRunning) {
            for (String symbol : symbolList) {
                // 给每支股票随机生成一个评价
                String status = "NORMAL";
                if (rand.nextGaussian() > 0.05) {
                    status = "POSITIVE";
                }
                srcCtx.collect(new Media(symbol, startTs + inc * 1000, status));
            }
            inc += 1;
            Thread.sleep(1000);
        }
    }

    // 停止发送数据
    @Override
    public void cancel() {
        isRunning = false;
    }
}
