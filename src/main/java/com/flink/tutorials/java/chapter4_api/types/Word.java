package com.flink.tutorials.java.chapter4_api.types;

public class Word {

    public String word;
    public int count;

    public Word() {}

    public Word(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public static Word of(String word, int count) {
        return new Word(word, count);
    }

    @Override
    public String toString() {
        return this.word + ": " + this.count;
    }
}
