package com.flink.tutorials.java.chapter2_basics.generics;

import java.util.ArrayList;

public class TypeErasure {
    public static void main(String[] args) {
        Class<?> strListClass = new ArrayList<String>().getClass();
        Class<?> intListClass = new ArrayList<Integer>().getClass();
        // The output of the following line: class java.util.ArrayList
        System.out.println(strListClass);
        // The output of the following line: class java.util.ArrayList
        System.out.println(intListClass);
        // The output of the following line: true
        System.out.println(strListClass.equals(intListClass));
    }
}
