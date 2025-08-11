package com.flink.tutorials.java.chapter8_sql;// 注意当前在 2.0-preview1 无法运行
// 保证 flink version为 1.20.0
// 因为 flink-connector-kafka 没有 2.0 preview1 的版本


//package com.flink.tutorials.java.chapter8_sql;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.NewTopic;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import java.util.Collections;
//import java.util.Properties;
//import java.util.Random;
//import java.util.concurrent.TimeUnit;
//
//public class UserBehaviorUVPerMin {
//
//    public static void ensureTopicExists(String topicName, int partitions, short replicationFactor) {
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//
//        try (AdminClient adminClient = AdminClient.create(props)) {
//            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
//            adminClient.createTopics(Collections.singleton(newTopic));
//        } catch (Exception e) {
//            System.out.println("确保Topic存在时发生错误: " + e.getMessage());
//        }
//    }
//
//    public static void generateAndSendData() {
//        ensureTopicExists("user_behavior", 1, (short)1);
//
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
//        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
//        props.put(ProducerConfig.ACKS_CONFIG, "1");
//
//        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
//
//        Random random = new Random();
//        String[] behaviors = {"pv", "buy", "cart", "fav"};
//
//        System.out.println("开始向Kafka写入数据...");
//
//        try {
//            for (int i = 0; i < 100; i++) {
//                long userId = 1000L + random.nextInt(100);
//                long itemId = 2000L + random.nextInt(200);
//                long categoryId = 100L + random.nextInt(20);
//                String behavior = behaviors[random.nextInt(behaviors.length)];
//                long timestamp = System.currentTimeMillis() / 1000;
//
//                String jsonData = String.format(
//                        "{\"user_id\":%d,\"item_id\":%d,\"category_id\":%d,\"behavior\":\"%s\",\"ts_raw\":%d}",
//                        userId, itemId, categoryId, behavior, timestamp);
//
//                ProducerRecord<String, String> record = new ProducerRecord<>("user_behavior", jsonData);
//                producer.send(record);
//
//                if (i % 20 == 0) {
//                    System.out.println("已发送 " + i + " 条记录");
//                    Thread.sleep(100);
//                }
//            }
//
//            producer.flush();
//            System.out.println("数据写入完成！");
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            producer.close();
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        System.out.println("第一阶段：生成数据并发送到Kafka");
//        generateAndSendData();
//
//        System.out.println("等待5秒确保数据写入Kafka...");
//        TimeUnit.SECONDS.sleep(5);
//
//        System.out.println("第二阶段：从Kafka读取数据并分析");
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
//                "    user_id BIGINT,\n" +
//                "    item_id BIGINT,\n" +
//                "    category_id BIGINT,\n" +
//                "    behavior STRING,\n" +
//                "    ts_raw BIGINT,\n" +
//                "    ts AS TO_TIMESTAMP_LTZ(ts_raw * 1000, 3),\n" +
//                "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND\n" +
//                ") WITH (\n" +
//                "    'connector' = 'kafka',\n" +
//                "    'topic' = 'user_behavior',\n" +
//                "    'scan.startup.mode' = 'earliest-offset',\n" +
//                "    'properties.zookeeper.connect' = 'localhost:2181',\n" +
//                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "    'properties.group.id' = 'testGroup',\n" +
//                "    'properties.auto.offset.reset' = 'earliest',\n" +
//                "    'properties.enable.auto.commit' = 'true',\n" +
//                "    'properties.consumer.poll.timeout.ms' = '10000',\n" +
//                "    'format' = 'json',\n" +
//                "    'json.fail-on-missing-field' = 'false',\n" +
//                "    'json.ignore-parse-errors' = 'true'\n" +
//                ")");
//
//        Table simpleUv = tEnv.sqlQuery("" +
//                "SELECT " +
//                "   user_id, " +
//                "   COUNT(*) OVER w AS uv " +
//                "FROM user_behavior " +
//                "WINDOW w AS (" +
//                "ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
//
//        Table cumulativeUv = tEnv.sqlQuery("" +
//                "SELECT time_str, MAX(uv) as max_uv FROM (" +
//                "SELECT " +
//                "   MAX(SUBSTR(DATE_FORMAT(ts, 'HH:mm'), 1, 4) || '0') OVER w AS time_str, " +
//                "   COUNT(DISTINCT user_id) OVER w AS uv " +
//                "FROM user_behavior " +
//                "WINDOW w AS (" +
//                "ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))" +
//                "GROUP BY time_str");
//
//        DataStream<Row> result = tEnv.toChangelogStream(cumulativeUv);
//        result.print();
//
//        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart());
//        env.execute("用户行为分析");
//    }
//}
