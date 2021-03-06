package com.bay.common;

/**
 * @Description: 全局常量类
 * Author by BayMin, Date on 2018/7/26.
 */
public class GlobalConstants {
    public static final String RUNNING_DATE = "running_date";
    public static final String DEFAULT_VALUE = "unknown";
    public static final String ALL_OF_VALUE = "all";
    public static final String URL = "jdbc:mysql://hadoop010:3306/result?useUnicode=true&characterEncoding=UTF-8"; // 写入中文时,需要指定编码类型
    public static final String DRIVER = "com.mysql.jdbc.Driver";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "19950116";
    public static final String PREFIX_OUTPUT = "output_";
    public static final String PREFIX_TOTAL = "total_";
    public static final String PREFIX_HOUR = "hour_";
    public static final int NUM_OF_BATCH = 50;
    public static final long DAY_OF_MILISECONDS = 86400000L;
}
