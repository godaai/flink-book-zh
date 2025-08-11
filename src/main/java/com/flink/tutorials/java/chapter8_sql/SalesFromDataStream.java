package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class SalesFromDataStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple3<Long, Long, Long>> itemList = new ArrayList<>();
        itemList.add(Tuple3.of(1L, 100L, 980L));
        itemList.add(Tuple3.of(2L, 99L, 992L));
        itemList.add(Tuple3.of(3L, 100L, 995L));
        itemList.add(Tuple3.of(4L, 99L, 999L));
        itemList.add(Tuple3.of(5L, 100L, 991L));
        itemList.add(Tuple3.of(6L, 99L, 989L));

        DataStream<Tuple3<Long, Long, Long>> itemSalesStream = env.fromData(itemList);
        
        // 使用Schema Builder定义表结构，包含处理时间属性
        Table itemSalesTable = tEnv.fromDataStream(
                itemSalesStream,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())       // item_id
                        .column("f1", DataTypes.BIGINT())       // category_id
                        .column("f2", DataTypes.BIGINT())       // sales
                        .columnByExpression("ts", "PROCTIME()")  // 处理时间属性
                        .build());

        tEnv.createTemporaryView("sales", itemSalesTable);

        Table windowSum = tEnv.sqlQuery("SELECT " +
                "f1 as category_id, " +
                "SUM(f2) OVER w AS sales_sum " +
                "FROM sales " +
                "WINDOW w AS (" +
                "PARTITION BY f1 " +
                "ORDER BY ts " +
                "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)");

        // 在Flink 2.0中，使用toChangelogStream替代toRetractStream
        DataStream<Row> result = tEnv.toChangelogStream(windowSum);
        result.print();

        env.execute("table api");
    }
}

// 输出
// +I[99, 992] 插入category_id为99，销售金额为992的记录 因为只有他自己
// +I[100, 980] 插入category_id为100，销售金额为980的记录 ↑
// +I[99, 1991] 插入category_id为99，销售金额为1991的记录 这时来了第二条记录，求和 992+999
// +I[100, 1975] 插入category_id为100，销售金额为1975的记录 来了第二条记录，求和 980+995
// +I[99, 1988] 插入category_id为99，销售金额为1988的记录 来了第三条记录，求和 1991+999
// +I[100, 1986] 插入category_id为100，销售金额为1986的记录 来了第三条记录，求和 1975+995