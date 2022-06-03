package com.flink.tutorials.java.chapter8_sql.function;


import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/**
 * 加权平均函数
 */
public class WeightedAvg extends AggregateFunction<Double, WeightedAvg.WeightedAvgAccum> {

    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    // 需要物化输出时，getValue方法会被调用
    @Override
    public Double getValue(WeightedAvgAccum acc) {
        if (acc.weight == 0) {
            return null;
        } else {
            return (double) acc.sum / acc.weight;
        }
    }

    // 新数据到达时，更新ACC
    public void accumulate(WeightedAvgAccum acc, long iValue, long iWeight) {
        acc.sum += iValue * iWeight;
        acc.weight += iWeight;
    }

    // 用于BOUNDED OVER WINDOW，将较早的数据剔除
    public void retract(WeightedAvgAccum acc, long iValue, long iWeight) {
        acc.sum -= iValue * iWeight;
        acc.weight -= iWeight;
    }

    // 将多个ACC合并为一个ACC
    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
        Iterator<WeightedAvgAccum> iter = it.iterator();
        while (iter.hasNext()) {
            WeightedAvgAccum a = iter.next();
            acc.weight += a.weight;
            acc.sum += a.sum;
        }
    }

    // 重置ACC
    public void resetAccumulator(WeightedAvgAccum acc) {
        acc.weight = 0l;
        acc.sum = 0l;
    }

    /**
     * 累加器 Accumulator
     * sum: 和
     * weight: 权重
     */
    public static class WeightedAvgAccum {
        public long sum = 0;
        public long weight = 0;
    }
}
