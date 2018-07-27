package com.qianfeng.analystic.model.dim.base;

import com.qianfeng.common.DateEnum;
import com.qianfeng.util.TimeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

/**
 * @Description: 时间维度类
 * Author by BayMin, Date on 2018/7/27.
 */
public class DateDimension extends BaseDimension {
    private int id;
    private int year;
    private int season;
    private int month;
    private int weeek;
    private int day;
    private Date calendar = new Date();
    private String type;

    public DateDimension(int year, int season, int month, int weeek, int day) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.weeek = weeek;
        this.day = day;
    }

    public DateDimension(int year, int season, int month, int weeek, int day, Date calendar) {
        this(year, season, month, weeek, day);
        this.calendar = calendar;
    }

    public DateDimension(int year, int season, int month, int weeek, int day, Date calendar, String type) {
        this(year, season, month, weeek, day, calendar);
        this.type = type;
    }

    public DateDimension(int id, int year, int season, int month, int weeek, int day, Date calendar, String type) {
        this(year, season, month, weeek, day, calendar, type);
        this.id = id;
    }

    /**
     * 根据时间戳和Type获取时间的维度
     */
    public static DateDimension buildDate(Long timestamp, DateEnum type) {
        int year = TimeUtil.getDateInfo(timestamp, type);
        Calendar calendar = Calendar.getInstance();
        calendar.clear(); // 先清除日历对象
        // 判断type的类型
        if (DateEnum.YEAR.equals(type)) {// 当年的1月1号这一天
            calendar.set(year, 0, 1);
            return new DateDimension(year, 1, 1, 0, 1, calendar.getTime(), type.type);
        }
        int season = TimeUtil.getDateInfo(timestamp, DateEnum.SEASON);
        if (DateEnum.SEASON.equals(type)) {// 当季度的第一个月的1号这一天
            int month = season * 3 - 2;
            calendar.set(year, month - 1, 1); // Calendar的月份需要-1
            return new DateDimension(year, season, month, 0, 1, calendar.getTime(), type.type);
        }
        int month = TimeUtil.getDateInfo(timestamp, DateEnum.MONTH);
        if (DateEnum.MONTH.equals(type)) { // 当月1号这一天
            calendar.set(year, month - 1, 1);
            return new DateDimension(year, season, month, 0, 1, calendar.getTime(), type.type);
        }
        int week = TimeUtil.getDateInfo(timestamp, DateEnum.WEEK);
        if (DateEnum.WEEK.equals(type)) { // 当周的第一天的0时0分0秒
            long firstDayOfWeek = TimeUtil.getFirstDayOfWeek(timestamp);
            year = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.YEAR);
            season = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.SEASON);
            month = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.MONTH);
            week = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.WEEK);
            int day = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.DAY);
            calendar.set(year, month - 1, 1);
            return new DateDimension(year, season, month, week, day, calendar.getTime(), type.type);
        }
        int day = TimeUtil.getDateInfo(timestamp, DateEnum.DAY);
        if (DateEnum.DAY.equals(type)) {
            calendar.set(year, month - 1, day);
            return new DateDimension(year, season, month, week, day, calendar.getTime(), type.type);
        }
        throw new RuntimeException("该类型暂时不支持获取时间维度." + type.type);
    }

    @Override
    public int compareTo(BaseDimension o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
