package com.flink.tutorials.java.api.utils.random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomSource implements SourceFunction<RandomId> {

    // Source是否正在运行
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<RandomId> sourceContext) throws Exception {
        Random rand = new Random();
        Integer id = rand.nextInt(10);
        long ts = System.currentTimeMillis();
        sourceContext.collect(RandomId.of(id, ts));
        Thread.sleep(rand.nextInt(2000));
    }

    // 停止发送数据
    @Override
    public void cancel() {
        isRunning = false;
    }

}
