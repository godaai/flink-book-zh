package com.flink.tutorials.java.exercise.chapter6;

import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;
import java.util.Properties;
import java.util.function.Consumer;

public class ItemKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ItemKafkaProducer.class);
    private static final File checkpoint = new File("checkpoint");

    public static void main(String[] args) {
        File userBehaviorFile = new File("user_behavior.log");
        Consumer<String> consumer = new ConsolePrinter();

        // parse arguments
        int argOffset = 0;
        while(argOffset < args.length) {

            String arg = args[argOffset++];
            switch (arg) {
                case "--input":
                    String basePath = args[argOffset++];
                    userBehaviorFile = new File(basePath);
                    break;
                case "--output":
                    String sink = args[argOffset++];
                    switch (sink) {
                        case "console":
                            consumer = new ConsolePrinter();
                            break;
                        case "kafka":
                            String brokers = args[argOffset++];
                            consumer = new KafkaProducer("item", brokers);
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown output configuration");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown parameter");
            }
        }

        long startLine = 0;
        if (checkpoint.exists()) {
            String line = null;
            try {
                line = FileUtils.readFileUtf8(checkpoint);
            } catch (IOException e) {
                LOGGER.error("exception", e);
            }
            if (!StringUtils.isNullOrWhitespaceOnly(line)) {
                startLine = Long.parseLong(line);
            }
        }

        if (!checkpoint.exists()) {
            try {
                checkpoint.createNewFile();
            } catch (IOException e) {
                LOGGER.error("exception", e);
            }
        }
//        checkpointState(startLine);

        try (InputStream inputStream = new FileInputStream(userBehaviorFile)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            boolean isFirstLine = true;
            long lastEventTs = 0;
            long currentLine = 0;
            while (reader.ready()) {
                String line = reader.readLine();
                if (currentLine < startLine) {
                    currentLine++;
                    continue;
                }
                currentLine++;
//                checkpointState(currentLine);
                String[] splits = line.split(",");
                long eventTs = Long.parseLong(splits[2])*1000;
                Instant instant = Instant.ofEpochMilli(eventTs);
                String output = String.format(
                        "{\"item_id\": \"%s\", \"price\":\"%s\", \"versionTs\": \"%s\"}",
                        splits[0],
                        splits[1],
                        instant.toString());
                consumer.accept(output);
                if (isFirstLine) {
                    lastEventTs = eventTs;
                    isFirstLine = false;
                }
                long timeDiff = eventTs - lastEventTs;
                if (timeDiff > 0) {
                    Thread.sleep(timeDiff);
                }
                lastEventTs = eventTs;
            }
            reader.close();
        } catch (IOException | InterruptedException e) {
            LOGGER.error("exception", e);
        }
    }

    private static void checkpointState(long lineState) {
        try {
            FileUtils.writeFileUtf8(checkpoint, String.valueOf(lineState));
        } catch (IOException e) {
            LOGGER.error("exception", e);
        }
    }

    public static class ConsolePrinter implements Consumer<String> {
        @Override
        public void accept(String s) {
            System.out.println(s);
        }
    }

    /**
     * Kafka Producer
     * 生成数据发送到某Topic种
     */
    public static class KafkaProducer implements Consumer<String> {

        private final String topic;
        private final org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> producer;

        public KafkaProducer(String kafkaTopic, String kafkaBrokers) {
            this.topic = kafkaTopic;
            this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(createKafkaProperties(kafkaBrokers));
        }

        @Override
        public void accept(String record) {
            // 将数据以JSON的形式发送到topic
            ProducerRecord<byte[], byte[]> kafkaRecord = new ProducerRecord<>(topic, record.getBytes());
            producer.send(kafkaRecord);
        }

        /**
         * 设置Kafka的参数
         */
        private static Properties createKafkaProperties(String brokers) {
            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
            return kafkaProps;
        }
    }
}
