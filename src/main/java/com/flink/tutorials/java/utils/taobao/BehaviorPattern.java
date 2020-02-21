package com.flink.tutorials.java.utils.taobao;

/**
 * 行为模式
 * 整个模式简化为两个行为
 * */
public class BehaviorPattern {

    public String firstBehavior;
    public String secondBehavior;

    public BehaviorPattern() {}

    public BehaviorPattern(String firstBehavior, String secondBehavior) {
        this.firstBehavior = firstBehavior;
        this.secondBehavior = secondBehavior;
    }

    @Override
    public String toString() {
        return "first: " + firstBehavior + ", second: " + secondBehavior;
    }
}
