package com.flink.tutorials.java.chapter8_sql.function;

import org.apache.flink.table.functions.TableFunction;

public class TableFunc extends TableFunction<String> {

    // 按#切分字符串，输出零到多行
    public void eval(String str) {
        if (str.contains("#")) {
            String[] arr = str.split("#");
            for (String i: arr) {
                collect(i);
            }
        }
    }
}
