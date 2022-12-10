package com.flink.tutorials.java.exercise.chapter8;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class IoTSQLDemo {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String sensorFilePath = IoTSQLDemo.class
                .getClassLoader().getResource("iot/sensor.csv")
                .getPath();

        String envFilePath = IoTSQLDemo.class
                .getClassLoader().getResource("iot/env.csv")
                .getPath();

        tEnv.executeSql("CREATE TABLE sensor (\n" +
                "  room STRING,\n" +
                "  node_id BIGINT,\n" +
                "  temp FLOAT,\n" +
                "  humidity FLOAT,\n" +
                "  light FLOAT,\n" +
                "  ts TIMESTAMP(3),\n" +
                "  WATERMARK FOR ts as ts - INTERVAL '5' SECOND" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',  -- 必填\n" +
                "  'connector.path' = '" + sensorFilePath + "',  -- 必填\n" +
                "  'format.type' = 'csv' -- 必填\n" +
                ")");

        tEnv.executeSql("CREATE TABLE env (\n" +
                "  room STRING,\n" +
                "  occupant INT,\n" +
                "  activity INT,\n" +
                "  door INT,\n" +
                "  win INT,\n" +
                "  ts TIMESTAMP(3),\n" +
                "  WATERMARK FOR ts as ts - INTERVAL '5' SECOND" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',  -- 必填\n" +
                "  'connector.path' = '" + envFilePath + "',  -- 必填\n" +
                "  'format.type' = 'csv' -- 必填\n" +
                ")");

        Table e = tEnv.sqlQuery("SELECT * FROM env");

        tEnv.executeSql("CREATE TABLE sensor_1min_avg (\n" +
                "  room STRING,\n" +
                "  avg_temp FLOAT,\n" +
                "  end_ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',  -- 必填\n" +
                "  'connector.path' = 'file:///tmp/sensor_1min_avg.csv',  -- 必填\n" +
                "  'format.type' = 'csv' -- 必填\n" +
                ")");

        tEnv.executeSql("CREATE TABLE sensor_env_data (\n" +
                "  room STRING,\n" +
                "  node_id BIGINT,\n" +
                "  temp FLOAT,\n" +
                "  occupant INT,\n" +
                "  activity INT,\n" +
                "  ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',  -- 必填\n" +
                "  'connector.path' = 'file:///tmp/sensor_env_data',  -- 必填\n" +
                "  'format.type' = 'csv' -- 必填\n" +
                ")");

        tEnv.executeSql("INSERT INTO sensor_1min_avg " +
                "SELECT " +
                "  room, " +
                "  AVG(temp) AS avg_temp," +
                "  TUMBLE_END(ts, INTERVAL '1' MINUTE) AS end_ts " +
                "FROM sensor " +
                "GROUP BY room, TUMBLE(ts, INTERVAL '1' MINUTE)");

        // 注册 Temporal Table Function
        tEnv.registerFunction(
                "env_table_func",
                e.createTemporalTableFunction("ts", "room"));

        String sqlQuery = "INSERT INTO sensor_env_data\n" +
                "SELECT \n" +
                "  sensor.room,\n" +
                "  sensor.node_id,\n" +
                "  sensor.temp,\n" +
                "  latest_env.occupant,\n" +
                "  latest_env.activity,\n" +
                "  sensor.ts\n" +
                "FROM " +
                "   sensor, LATERAL TABLE(env_table_func(sensor.ts)) AS latest_env\n" +
                "WHERE sensor.room = latest_env.room";

        // executeSql()是一个异步方法，会将计算任务提交
        TableResult result = tEnv.executeSql(sqlQuery);
        // 使用Intellij Idea调试，需要加上的代码，以等待result返回结果
        result.getJobClient().get()
                .getJobExecutionResult().get();

        // 由于executeSql()已经将计算任务提交，因此无需execute()方法
        // env.execute("table api");
    }
}
