package com.flink.tutorials.java.chapter2_basics.generics;

/**
 * 一个泛型数组列表的实现
 */
public class MyArrayList<T> {

    private final int capacity;
    private Object[] elements;

    /**
     * 构造函数，初始化数组容量
     */
    public MyArrayList(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        this.elements = new Object[capacity];
    }

    /**
     * 在指定位置设置元素
     */
    public void set(T element, int position) {
        if (position < 0 || position >= capacity) {
            throw new IndexOutOfBoundsException("Position must be between 0 and " + (capacity - 1));
        }
        elements[position] = element;
    }

    /**
     * 获取指定位置的元素
     */
    public T get(int position) {
        if (position < 0 || position >= capacity) {
            throw new IndexOutOfBoundsException("Position must be between 0 and " + (capacity - 1));
        }
        return (T) elements[position];
    }

    /**
     * 获取数组容量
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * 获取指定位置的元素，如果位置越界则返回默认值
     */
    public T getOrDefault(int position, T defaultValue) {
        if (position < 0 || position >= capacity) {
            return defaultValue;
        }
        return (T) elements[position];
    }

    /**
     * 泛型方法示例：处理两个元素
     */
    public <E, K> E processElements(E element1, K element2) {
        // 这里只是简单地返回第一个元素
        // 实际应用中可以添加更复杂的处理逻辑
        return element1;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("[");
        for (int i = 0; i < capacity; i++) {
            if (elements[i] != null) {
                result.append(elements[i].toString());
                if (i < capacity - 1) {
                    result.append(", ");
                }
            }
        }
        result.append("]");
        return result.toString();
    }

    public <E> void printInfo(E element) {
        System.out.println(element.toString());
    }

    public static void main(String[] args){

        MyArrayList<String> strList = new MyArrayList<>(2);
        strList.set("first", 0);
        strList.set("second", 1);

        MyArrayList<Integer> intList = new MyArrayList<>(2);
        intList.set(11, 0);
        intList.set(22, 1);

        System.out.println(strList);
        System.out.println(intList);

        intList.printInfo("function");
    }

}
