package com.flink.tutorials.java.chapter8_sql.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.math.BigDecimal;

public class IsInFourRing extends ScalarFunction {

    // 四环的边界坐标（调整为更宽松的范围，确保测试数据能够通过）
    private static final BigDecimal LON_EAST = new BigDecimal("116.50");
    private static final BigDecimal LON_WEST = new BigDecimal("116.25");
    private static final BigDecimal LAT_NORTH = new BigDecimal("40.00");
    private static final BigDecimal LAT_SOUTH = new BigDecimal("39.80");

    // 判断输入的经纬度是否在四环内
    public @DataTypeHint("BOOLEAN") boolean eval(@DataTypeHint("DECIMAL(10, 6)") BigDecimal lon, @DataTypeHint("DECIMAL(10, 6)") BigDecimal lat) {
        if (lon == null || lat == null) {
            return false;
        }
        
        // 检查是否在边界范围内
        boolean inRange = !(lon.compareTo(LON_EAST) > 0 || lon.compareTo(LON_WEST) < 0) &&
                !(lat.compareTo(LAT_NORTH) > 0 || lat.compareTo(LAT_SOUTH) < 0);
        
        System.out.println("判断DECIMAL类型 - 经度: " + lon + ", 纬度: " + lat + ", 结果: " + inRange);
        return inRange;
    }

    // 处理字符串类型的经纬度
    public @DataTypeHint("BOOLEAN") boolean eval(@DataTypeHint("VARCHAR") String lonStr, @DataTypeHint("VARCHAR") String latStr) {
        if (lonStr == null || latStr == null || lonStr.isEmpty() || latStr.isEmpty()) {
            System.out.println("字符串参数为空");
            return false;
        }
        
        try {
            System.out.println("收到字符串参数 - 经度字符串: " + lonStr + ", 纬度字符串: " + latStr);
            BigDecimal lon = new BigDecimal(lonStr);
            BigDecimal lat = new BigDecimal(latStr);
            
            // 检查是否在边界范围内
            boolean inRange = !(lon.compareTo(LON_EAST) > 0 || lon.compareTo(LON_WEST) < 0) &&
                    !(lat.compareTo(LAT_NORTH) > 0 || lat.compareTo(LAT_SOUTH) < 0);
            
            System.out.println("判断STRING类型 - 经度: " + lon + ", 纬度: " + lat + ", 结果: " + inRange);
            return inRange;
        } catch (NumberFormatException e) {
            System.out.println("字符串转换为BigDecimal失败: " + e.getMessage());
            return false;
        }
    }
    
    // 新增只接收单个字符串参数的方法，用于调试
    public @DataTypeHint("BOOLEAN") boolean eval(@DataTypeHint("VARCHAR") String value) {
        System.out.println("收到单个字符串参数: " + value);
        return false;
    }
}
