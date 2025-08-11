package com.flink.tutorials.java.chapter4_api.types;

import com.flink.tutorials.java.utils.stock.StockPrice;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TypeCheck {

    public static void main(String[] args) {

        ExecutionConfig executionConfig = new ExecutionConfig();
        var serializerConfig = executionConfig.getSerializerConfig();

        // 检查 StockPrice 类的序列化器
        System.out.println("StockPrice Serializer: " +
                TypeInformation.of(StockPrice.class).createSerializer(serializerConfig));

        // 检查 StockPriceNoGeterSeter 类的序列化器
        System.out.println("StockPriceNoGeterSeter Serializer: " +
                TypeInformation.of(StockPriceNoGeterSeter.class).createSerializer(serializerConfig));

        // 检查 StockPriceNoConstructor 类的序列化器
        System.out.println("StockPriceNoConstructor Serializer: " +
                TypeInformation.of(StockPriceNoConstructor.class).createSerializer(serializerConfig));
    }
}