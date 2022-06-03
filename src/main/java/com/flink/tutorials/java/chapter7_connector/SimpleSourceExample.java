package com.flink.tutorials.java.chapter7_connector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleSourceExample {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 访问 http://localhost:8082 可以看到Flink Web UI
        conf.setInteger(RestOptions.PORT, 8082);
        // 创建本地执行环境，并行度为2
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, conf);
        DataStream<Tuple2<String, Integer>> countStream = env.addSource(new SimpleSource());
        System.out.println("parallelism: " + env.getParallelism());

        countStream.print();
        env.execute("source");
    }

    private static class SimpleSource implements SourceFunction<Tuple2<String, Integer>> {

        private int offset = 0;
        private boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            while (isRunning) {
                Thread.sleep(500);
                ctx.collect(new Tuple2<>("" + offset, offset));
                offset++;
                if (offset == 1000) {
                    isRunning = false;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
