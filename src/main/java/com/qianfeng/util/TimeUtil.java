package com.qianfeng.util;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description: 时间的工具类
 * Author by BayMin, Date on 2018/7/26.
 */
public class TimeUtil {
    private static final Logger logger = Logger.getLogger(TimeUtil.class);
    private static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd"; // 默认时间格式

    /**
     * 验证日期是否合法
     */
    public static boolean isValidateDate(String date) {
        Matcher matcher = null;
        boolean res = false;
        String regexp = "^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}";
        if (date != null) {
            Pattern pattern = Pattern.compile(regexp);
            matcher = pattern.matcher(date);
        }
        if (matcher != null)
            res = matcher.matches();
        return res;
    }

    /**
     * 获取昨天的日期
     * 不传格式则使用默认时间格式
     */
    public static String getYesterdayDate() {
        return getYesterdayDate(DEFAULT_DATE_FORMAT);
    }

    public static String getYesterdayDate(String dateformat) {
        SimpleDateFormat sdf = new SimpleDateFormat(dateformat);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }

    /**
     * 将字符串的日期转换成时间戳
     * 不传格式则为默认格式
     */
    public static long parserString2Long(String date) {
        return parserString2Long(date, DEFAULT_DATE_FORMAT);
    }

    public static long parserString2Long(String date, String parttern) {
        SimpleDateFormat sdf = new SimpleDateFormat(parttern);
        Date dt = null;
        try {
            dt = sdf.parse(date);
        } catch (ParseException e) {
            logger.warn("解析字符串的date为时间戳异常", e);
        }
        return dt == null ? 0 : dt.getTime();
    }

    /**
     * 将时间戳转换成日期
     * 不传格式则为默认格式
     */
    public static String parserLong2String(Long timestamp) {
        return parserLong2String(timestamp, DEFAULT_DATE_FORMAT);
    }

    public static String parserLong2String(Long timestamp, String format) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp); // 毫秒
        return new SimpleDateFormat(format).format(calendar.getTime());
    }

    /**
     * 测试
     */
//    public static void main(String[] args) {
//        System.out.println(isValidateDate("2018-7-26"));
//        System.out.println(getYesterdayDate());
//        System.out.println(parserString2Long("2018-07-26"));
//        System.out.println(parserLong2String(parserString2Long("2018-07-25")));
//    }
}
