package com.flink.tutorials.java.chapter2_basics.lambda;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JavaStreamExample {

    public static void main( String[] args ) {
        List<String> strings = Arrays.asList(
                "abc", "", "bc", "12345",
                "efg", "abcd","", "jkl");

        List<Integer> lengths = strings
                .stream()
                .filter(string -> !string.isEmpty())
                .map(s -> s.length())
                .collect(Collectors.toList());

        lengths.forEach((s) -> System.out.println(s));
    }

}
