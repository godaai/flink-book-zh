package com.flink.tutorials.java.api.types;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TypeCheck {

    public static void main(String[] args) {

        System.out.println(TypeInformation.of(StockPrice.class).createSerializer(new ExecutionConfig()));

        System.out.println(TypeInformation.of(StockPrice1.class).createSerializer(new ExecutionConfig()));

        System.out.println(TypeInformation.of(StockPrice2.class).createSerializer(new ExecutionConfig()));
    }
}
