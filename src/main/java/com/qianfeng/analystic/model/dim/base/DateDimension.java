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
    private int week;
    private int day;
    private Date calendar = new Date();
    private String type; // 什么类型的指标

    public DateDimension() {
    }

    public DateDimension(int year, int season, int month, int weeek, int day) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = weeek;
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
        int year = TimeUtil.getDateInfo(timestamp, DateEnum.YEAR);
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
        if (o == this)
            return 0;
        DateDimension other = (DateDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0)
            return tmp;
        tmp = this.year - other.year;
        if (tmp != 0)
            return tmp;
        tmp = this.season - other.season;
        if (tmp != 0)
            return tmp;
        tmp = this.month - other.month;
        if (tmp != 0)
            return tmp;
        tmp = this.week - other.week;
        if (tmp != 0)
            return tmp;
        tmp = this.day - other.day;
        if (tmp != 0)
            return tmp;
//        tmp = this.calendar.compareTo(other.calendar);
//        if (tmp != 0)
//            return tmp;
        tmp = this.type.compareTo(other.type);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeInt(this.year);
        out.writeInt(this.season);
        out.writeInt(this.month);
        out.writeInt(this.week);
        out.writeInt(this.day);
        out.writeLong(this.calendar.getTime()); // Date类型写成时间戳
        out.writeUTF(this.type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.year = in.readInt();
        this.season = in.readInt();
        this.month = in.readInt();
        this.week = in.readInt();
        this.day = in.readInt();
        this.calendar.setTime(in.readLong()); // Date类型读时间戳
        this.type = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateDimension that = (DateDimension) o;

        if (id != that.id) return false;
        if (year != that.year) return false;
        if (season != that.season) return false;
        if (month != that.month) return false;
        if (week != that.week) return false;
        if (day != that.day) return false;
        if (!calendar.equals(that.calendar)) return false;
        return type.equals(that.type);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + year;
        result = 31 * result + season;
        result = 31 * result + month;
        result = 31 * result + week;
        result = 31 * result + day;
        result = 31 * result + calendar.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeeek() {
        return week;
    }

    public void setWeeek(int weeek) {
        this.week = weeek;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
