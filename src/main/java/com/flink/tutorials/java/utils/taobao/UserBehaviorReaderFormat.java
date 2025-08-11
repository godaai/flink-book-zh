package com.flink.tutorials.java.utils.taobao;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class UserBehaviorReaderFormat extends SimpleStreamFormat<UserBehavior> {

    @Override
    public Reader<UserBehavior> createReader(Configuration config, FSDataInputStream stream) throws IOException {
        return new UserBehaviorReader(stream);
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return TypeInformation.of(UserBehavior.class);
    }

    private static class UserBehaviorReader implements Reader<UserBehavior> {

        private final BufferedReader reader;
        private long lastEventTs = 0;

        public UserBehaviorReader(FSDataInputStream stream) {
            this.reader = new BufferedReader(new InputStreamReader(stream));
        }

        @Override
        public UserBehavior read() throws IOException {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }

            String[] itemStrArr = line.split(",");
            long eventTs = Long.parseLong(itemStrArr[4]);

            // 模拟时间间隔
            if (lastEventTs > 0) {
                long timeDiff = eventTs - lastEventTs;
                if (timeDiff > 0) {
                    try {
                        Thread.sleep(timeDiff * 1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Thread interrupted", e);
                    }
                }
            }
            lastEventTs = eventTs;

            return UserBehavior.of(
                    Long.parseLong(itemStrArr[0]),
                    Long.parseLong(itemStrArr[1]),
                    Integer.parseInt(itemStrArr[2]),
                    itemStrArr[3],
                    eventTs
            );
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}