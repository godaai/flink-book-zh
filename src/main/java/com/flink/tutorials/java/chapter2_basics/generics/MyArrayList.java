package com.flink.tutorials.java.chapter2_basics.generics;

public class MyArrayList<T> {

    private int size;
    T[] elements;

    public MyArrayList(int capacity) {
        this.size = capacity;
        this.elements = (T[]) new Object[capacity];
    }

    public void set(T element, int position) {
        elements[position] = element;
    }

    @Override
    public String toString() {
        String result = "";
        for (int i = 0; i < size; i++) {
            result += elements[i].toString();
        }
        return result;
    }

    public <E> void printInfo(E element) {
        System.out.println(element.toString());
    }

    public static void main(String[] args){

        MyArrayList<String> strList = new MyArrayList<String>(2);
        strList.set("first", 0);
        strList.set("second", 1);

        MyArrayList<Integer> intList = new MyArrayList<Integer>(2);
        intList.set(11, 0);
        intList.set(22, 1);

        System.out.println(strList.toString());
        System.out.println(intList.toString());

        intList.printInfo("function");
    }

}
