package com.flink.tutorials.java.chapter4_api.types;

import com.flink.tutorials.java.utils.stock.StockPrice;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TypeCheck {

    public static void main(String[] args) {

        System.out.println(TypeInformation.of(StockPrice.class).createSerializer(new ExecutionConfig()));

        System.out.println(TypeInformation.of(StockPriceNoGeterSeter.class).createSerializer(new ExecutionConfig()));

        System.out.println(TypeInformation.of(StockPriceNoConstructor.class).createSerializer(new ExecutionConfig()));
    }
}
