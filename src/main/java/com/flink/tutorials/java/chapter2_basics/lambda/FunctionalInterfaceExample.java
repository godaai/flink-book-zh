package com.flink.tutorials.java.chapter2_basics.lambda;

@FunctionalInterface
interface AddInterface<T> {
    T add(T a, T b);
}

public class FunctionalInterfaceExample {

    public static void main( String[] args ) {

        AddInterface<Integer> addInt = (Integer a, Integer b) -> a + b;
        AddInterface<Double> addDouble = (Double a, Double b) -> a + b;

        int intResult;
        double doubleResult;

        intResult = addInt.add(1, 2);
        System.out.println("Lambda int add = " + intResult);

        doubleResult = addDouble.add(1.1d, 2.2d);
        System.out.println("Lambda double  add = " + doubleResult);

        doubleResult = new MyAdd().add(1.1d, 2.2d);
        System.out.println("Class implementation add = " + doubleResult);

        doubleResult = new AddInterface<Double>(){
            @Override
            public Double add(Double a, Double b) {
                return a + b;
            }
        }.add(1d, 2d);

        System.out.println("Anonymous function add = " + doubleResult);
    }

    public static class MyAdd implements AddInterface<Double> {
        @Override
        public Double add(Double a, Double b) {
            return a + b;
        }
    }
}
