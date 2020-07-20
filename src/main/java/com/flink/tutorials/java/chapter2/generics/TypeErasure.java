package com.flink.tutorials.java.chapter2.generics;

import java.util.ArrayList;

public class TypeErasure {
    public static void main(String[] args) {
        Class<?> strListClass = new ArrayList<String>().getClass();
        Class<?> intListClass = new ArrayList<Integer>().getClass();
        // 输出：class java.util.ArrayList
        System.out.println(strListClass);
        // 输出：class java.util.ArrayList
        System.out.println(intListClass);
        // 输出：true
        System.out.println(strListClass.equals(intListClass));
    }
}
