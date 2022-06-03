package com.flink.tutorials.java.chapter8_sql.function;

import org.apache.flink.table.functions.ScalarFunction;

public class IsInFourRing extends ScalarFunction {

    private static double LON_EAST = 116.48;
    private static double LON_WEST = 116.27;
    private static double LAT_NORTH = 39.988;
    private static double LAT_SOUTH = 39.83;

    // 判断输入的经纬度是否在四环内
    public boolean eval(double lon, double lat) {
        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

    public boolean eval(float lon, float lat) {
        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

    public boolean eval(String lonStr, String latStr) {
        double lon = Double.parseDouble(lonStr);
        double lat = Double.parseDouble(latStr);
        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }
}
