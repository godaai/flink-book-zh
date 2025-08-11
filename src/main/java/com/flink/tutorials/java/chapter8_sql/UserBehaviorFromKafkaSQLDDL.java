package com.flink.tutorials.java.chapter8_sql;// 注意当前在 2.0-preview1 无法运行
// 保证 flink version为 1.20.0
// 因为 flink-connector-kafka 没有 2.0 preview1 的版本

//package com.flink.tutorials.java.chapter8_sql;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.ExplainDetail;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.CreateTopicsResult;
//import org.apache.kafka.clients.admin.NewTopic;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.KafkaFuture;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import java.time.Duration;
//import java.util.Collections;
//import java.util.Properties;
//import java.util.concurrent.ExecutionException;
//import java.util.stream.IntStream;
//
//public class UserBehaviorFromKafkaSQLDDL {
//
//    // Kafka配置常量
//    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
//    private static final String TOPIC_NAME = "user_behavior";
//
//    /**
//     * 创建Kafka主题并发送示例数据
//     */
//    private static void prepareKafkaTopic() {
//        System.out.println("开始准备Kafka主题和测试数据...");
//        Properties adminProps = new Properties();
//        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//
//        try (AdminClient adminClient = AdminClient.create(adminProps)) {
//            // 检查主题是否已存在
//            boolean topicExists = false;
//            try {
//                topicExists = adminClient.listTopics().names().get().contains(TOPIC_NAME);
//            } catch (InterruptedException | ExecutionException e) {
//                System.err.println("检查主题存在时出错: " + e.getMessage());
//            }
//
//            // 如果主题不存在，则创建
//            if (!topicExists) {
//                System.out.println("主题不存在，正在创建...");
//                NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
//                CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
//
//                try {
//                    KafkaFuture<Void> future = result.values().get(TOPIC_NAME);
//                    future.get(); // 等待主题创建完成
//                    System.out.println("主题创建成功!");
//                } catch (InterruptedException | ExecutionException e) {
//                    System.err.println("创建主题时出错: " + e.getMessage());
//                    return;
//                }
//            } else {
//                System.out.println("主题 '" + TOPIC_NAME + "' 已存在.");
//            }
//
//            // 发送测试数据到主题
//            sendSampleData();
//
//        }
//    }
//
//    /**
//     * 发送示例用户行为数据到Kafka主题
//     */
//    private static void sendSampleData() {
//        System.out.println("开始发送示例数据到Kafka主题...");
//        Properties producerProps = new Properties();
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
//            // 发送10条示例用户行为数据
//            long now = System.currentTimeMillis() / 1000; // 当前时间（秒）
//
//            String[] behaviors = {"pv", "buy", "cart", "fav"};
//
//            IntStream.range(0, 10).forEach(i -> {
//                // 用户行为JSON数据
//                long userId = 1000 + i % 3;  // 用户ID循环使用1000, 1001, 1002
//                long itemId = 2000 + i;      // 商品ID
//                long categoryId = 100 + i % 5; // 分类ID
//                String behavior = behaviors[i % behaviors.length]; // 行为类型
//                long ts = now + i;           // 时间戳递增
//
//                String jsonData = String.format(
//                    "{\"user_id\":%d,\"item_id\":%d,\"category_id\":%d,\"behavior\":\"%s\",\"ts\":%d}",
//                    userId, itemId, categoryId, behavior, ts
//                );
//
//                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, String.valueOf(i), jsonData);
//                producer.send(record, (metadata, exception) -> {
//                    if (exception == null) {
//                        System.out.println("数据已发送: " + jsonData);
//                    } else {
//                        System.err.println("发送数据时出错: " + exception.getMessage());
//                    }
//                });
//            });
//
//            // 确保所有消息都已发送
//            producer.flush();
//            System.out.println("所有测试数据已发送完成!");
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        // 先准备Kafka主题和数据
//        prepareKafkaTopic();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        // 设置状态保留时间 - 使用 Duration 替代 Time
//        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(1));
//
//        // 使用 Flink 2.0 的 Kafka 连接器语法
//        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
//                "    user_id BIGINT,\n" +
//                "    item_id BIGINT,\n" +
//                "    category_id BIGINT,\n" +
//                "    behavior STRING,\n" +
//                "    ts TIMESTAMP(3),\n" +
////                "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
//                "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
//                ") WITH (\n" +
//                "    'connector' = 'kafka',  -- 使用 kafka connector\n" +
//                "    'topic' = 'user_behavior',  -- kafka topic\n" +
//                "    'scan.startup.mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
//                "    'properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
//                "    'format' = 'json',  -- 数据源格式为 json\n" +
//                "    'json.timestamp-format.standard' = 'SQL',  -- 使用SQL标准时间格式\n" +
//                "    'json.fail-on-missing-field' = 'false',  -- 字段缺失时不失败\n" +
//                "    'json.ignore-parse-errors' = 'true'  -- 忽略解析错误\n" +
//                ")");
//
//        Table groupByUserId = tEnv.sqlQuery("SELECT user_id, COUNT(behavior) AS behavior_cnt FROM user_behavior GROUP BY user_id");
//        DataStream<Row> groupByUserIdResult = tEnv.toChangelogStream(groupByUserId);
//        groupByUserIdResult.print();
//
//        // 获取ExplainDetail
//        String explanation = groupByUserId.explain(ExplainDetail.CHANGELOG_MODE);
//        System.out.println(explanation);
//
//        Table tumbleGroupByUserId = tEnv.sqlQuery("SELECT \n" +
//                "\tuser_id, \n" +
//                "\tCOUNT(behavior) AS behavior_cnt, \n" +
//                "\tTUMBLE_END(ts, INTERVAL '10' SECOND) AS end_ts \n" +
//                "FROM user_behavior\n" +
//                "GROUP BY user_id, TUMBLE(ts, INTERVAL '10' SECOND)");
//
//        // 更新内联查询，使用更现代的语法
//        Table inlineGroupByUserId = tEnv.sqlQuery("" +
//                "SELECT " +
//                "    window_end," +
//                "    user_id," +
//                "    SUM(cnt) AS total_cnt " +
//                "FROM (" +
//                "SELECT \n" +
//                "\tuser_id, \n" +
//                "\tCOUNT(behavior) AS cnt, \n" +
//                "\tTUMBLE_START(ts, INTERVAL '10' SECOND) AS window_start, \n" +
//                "\tTUMBLE_END(ts, INTERVAL '10' SECOND) AS window_end \n" +
//                "FROM user_behavior\n" +
//                "GROUP BY user_id, TUMBLE(ts, INTERVAL '10' SECOND))" +
//                "GROUP BY window_end, user_id");
//
//        DataStream<Row> result = tEnv.toChangelogStream(inlineGroupByUserId);
//        // 如需查看打印结果，可将注释打开
//        // result.print();
//
//        System.out.println("当前时区: " + tEnv.getConfig().getLocalTimeZone());
//
//        env.execute("table api");
//    }
//}
