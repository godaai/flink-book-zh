package com.flink.tutorials.java.chapter2_basics;

/**
 * This class demonstrates method overloading in Java.
 * */

public class Overloading {

    // no input parameters, return a int
    // 无参数 返回值为int
    public int test(){
        System.out.println("test");
        return 1;
    }

    // one input parameter `a`
    // 有一个参数 `a`
    public void test(int a){
        System.out.println("test " + a);
    }

    // two input parameters `a` and `b`, return a String
    // 有两个参数和一个返回值
    public String test(int a, String s){
        System.out.println("test " + a  + " " + s);
        return a + " " + s;
    }

    public static void main(String[] args) {
        Overloading o = new Overloading();
        System.out.println(o.test());
        o.test(1);
        System.out.println(o.test(1,"test3"));
    }
}
