package com.flink.tutorials.java.chapter8_sql.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.time.Duration;

public class TimeDiff extends ScalarFunction {

    public @DataTypeHint("BIGINT") long eval(@DataTypeHint("TIMESTAMP(3)") Timestamp first, @DataTypeHint("TIMESTAMP(3)") Timestamp second) {
        return Duration.between(first.toLocalDateTime(), second.toLocalDateTime()).toMillis();
    }
}
