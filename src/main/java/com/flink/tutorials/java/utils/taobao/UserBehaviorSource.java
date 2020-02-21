package com.flink.tutorials.java.utils.taobao;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class UserBehaviorSource implements SourceFunction<UserBehavior> {

    private boolean isRunning = true;
    private String path;
    private InputStream streamSource;

    public UserBehaviorSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<UserBehavior> sourceContext) throws Exception {
        // 从项目的resources目录获取输入
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));
        String line;
        boolean isFirstLine = true;
        long timeDiff = 0;
        long lastEventTs = 0;
        while (isRunning && (line = br.readLine()) != null) {
            String[] itemStrArr = line.split(",");
            long eventTs = Long.parseLong(itemStrArr[4]);
            if (isFirstLine) {
                // 从第一行数据提取时间戳
                lastEventTs = eventTs;
                isFirstLine = false;
            }
            UserBehavior userBehavior = UserBehavior.of(Long.parseLong(itemStrArr[0]),
                    Long.parseLong(itemStrArr[1]), Integer.parseInt(itemStrArr[2]),
                    itemStrArr[3], eventTs);
            // 输入文件中的时间戳是从小到大排列的
            // 新读入的行如果比上一行大，sleep，这样来模拟一个有时间间隔的输入流
            timeDiff = eventTs - lastEventTs;
            if (timeDiff > 0)
                Thread.sleep(timeDiff * 1000);
            sourceContext.collect(userBehavior);
            lastEventTs = eventTs;
        }
    }

    // 停止发送数据
    @Override
    public void cancel() {
        try {
            streamSource.close();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        isRunning = false;
    }

}
