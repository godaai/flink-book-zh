package com.flink.tutorials.java.exercise.chapter7;

import com.flink.tutorials.java.utils.stock.StockPrice;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * This is the Source to simulate a stock market tick data.
 * We read data from an existing file which contains the market data.
 * We use `CheckpointedFunction` to checkpoint the file reader offset.
 */
public class StockSourceCkpt implements SourceFunction<StockPrice>, CheckpointedFunction {

    private int offset;
    private ListState<Integer> offsetState;

    private boolean isRunning = true;

    // datase filepath
    private String path;
    private InputStream streamSource;

    public StockSourceCkpt(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<StockPrice> sourceContext) throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
        // read the file from the project's resources folder
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));
        String line;
        boolean isFirstLine = true;
        long timeDiff = 0;
        long lastEventTs = 0;
        while (isRunning && (line = br.readLine()) != null) {
            String[] itemStrArr = line.split(",");
            LocalDateTime dateTime = LocalDateTime.parse(itemStrArr[1] + " " + itemStrArr[2], formatter);
            long eventTs = Timestamp.valueOf(dateTime).getTime();
            if (isFirstLine) {
                // extract timestamp from the first line of the file
                lastEventTs = eventTs;
                isFirstLine = false;
            }
            StockPrice stock = StockPrice.of(itemStrArr[0], Double.parseDouble(itemStrArr[3]), eventTs, Integer.parseInt(itemStrArr[4]));
            // the timestamp in the file is in ascending order
            // use `sleep()` to simulate the data stream with time intervals
            // without `sleep()`, Source will read all the data at once
            timeDiff = eventTs - lastEventTs;
            if (timeDiff > 0)
                Thread.sleep(timeDiff);
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(stock);
                offset++;
            }

            lastEventTs = eventTs;
        }
    }

    // stop sending data
    @Override
    public void cancel() {
        try {
            streamSource.close();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext snapshotContext) throws
            Exception {
        // clear previous state
        offsetState.clear();
        // update the lastest offset into offsetState
        offsetState.add(offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext initializationContext) throws Exception {
        // initialize offsetState
        ListStateDescriptor<Integer> desc = new
                ListStateDescriptor<Integer>("offset", Types.INT);
        offsetState =
                initializationContext.getOperatorStateStore().getListState(desc);

        Iterable<Integer> iter = offsetState.get();
        if (iter == null || !iter.iterator().hasNext()) {
            // if it's the first time to call `initializeState()
            // set offset to 0
            offset = 0;
        } else {
            // it it's recovered from previous checkpoint
            offset = iter.iterator().next();
        }
    }

}
