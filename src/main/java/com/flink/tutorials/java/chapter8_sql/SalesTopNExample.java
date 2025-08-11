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

public class SalesTopNExample {

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
                        .columnByExpression("time", "PROCTIME()")  // 处理时间属性
                        .build());

        tEnv.createTemporaryView("sales", itemSalesTable);

        Table topN = tEnv.sqlQuery(
                "SELECT * " +
                        "FROM (" +
                        "   SELECT *," +
                        "       ROW_NUMBER() OVER (PARTITION BY f1 ORDER BY f2 DESC) as row_num" +
                        "   FROM sales)" +
                        "WHERE row_num <= 3");
        
        // 在Flink 2.0中，使用toChangelogStream替代toRetractStream
        DataStream<Row> result = tEnv.toChangelogStream(topN);
        result.print();

        env.execute("table api");
    }
}

// 输出
// +I[2, 99, 992, 2025-03-17T08:52:22.911Z, 1]       插入第一条category_id=99的记录，此时只有该记录，排名第1
// +I[1, 100, 980, 2025-03-17T08:52:22.911Z, 1]      插入第一条category_id=100的记录，此时只有该记录，排名第1
// -U[2, 99, 992, 2025-03-17T08:52:22.920Z, 1]       撤回之前category_id=99的记录排名
// -U[1, 100, 980, 2025-03-17T08:52:22.920Z, 1]      撤回之前category_id=100的记录排名
// +U[4, 99, 999, 2025-03-17T08:52:22.920Z, 1]       更新category_id=99的第一名为item_id=4，销售额999
// +U[3, 100, 995, 2025-03-17T08:52:22.920Z, 1]      更新category_id=100的第一名为item_id=3，销售额995
// +I[1, 100, 980, 2025-03-17T08:52:22.920Z, 2]      插入原先的记录作为category_id=100的第二名
// +I[2, 99, 992, 2025-03-17T08:52:22.920Z, 2]       插入原先的记录作为category_id=99的第二名
// +I[6, 99, 989, 2025-03-17T08:52:22.921Z, 3]       插入category_id=99的第三名，item_id=6，销售额989
// -U[1, 100, 980, 2025-03-17T08:52:22.921Z, 2]      撤回之前category_id=100的第二名记录
// +U[5, 100, 991, 2025-03-17T08:52:22.921Z, 2]      更新category_id=100的第二名为item_id=5，销售额991
// +I[1, 100, 980, 2025-03-17T08:52:22.921Z, 3]      插入原先的记录作为category_id=100的第三名
