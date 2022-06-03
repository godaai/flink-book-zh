package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class SalesFromDataStream {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        List<Tuple3<Long, Long, Long>> itemList = new ArrayList<>();
        itemList.add(Tuple3.of(1L, 100L, 980L));
        itemList.add(Tuple3.of(2L, 99L, 992L));
        itemList.add(Tuple3.of(3L, 100L, 995L));
        itemList.add(Tuple3.of(4L, 99L, 999L));
        itemList.add(Tuple3.of(5L, 100L, 991L));
        itemList.add(Tuple3.of(6L, 99L, 989L));

        DataStream<Tuple3<Long, Long, Long>> itemSalesStream = env.fromCollection(itemList);
        Table itemSalesTable = tEnv.fromDataStream(itemSalesStream, "item_id, category_id, sales, ts.proctime");

        tEnv.createTemporaryView("sales", itemSalesTable);

        Table windowSum = tEnv.sqlQuery("SELECT " +
                "category_id, " +
                "SUM(sales) OVER w AS sales_sum " +
                "FROM sales " +
                "WINDOW w AS (" +
                "PARTITION BY category_id " +
                "ORDER BY ts " +
                "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)");

        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(windowSum, Row.class);
        result.print();

        env.execute("table api");
    }
}
