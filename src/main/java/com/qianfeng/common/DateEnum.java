package com.qianfeng.common;

/**
 * @Description: 时间枚举
 * Author by BayMin, Date on 2018/7/27.
 */
public enum DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public final String type;

    DateEnum(String type) {
        this.type = type;
    }

    /**
     * 根据Type获取时间枚举
     */
    public DateEnum valueOfType(String type) {
        for (DateEnum date : values()) {
            if (type.equals(date.type))
                return date;
        }
        throw new RuntimeException("暂不支持该类型获取时间枚举" + type);
    }
}
