package com.zb8.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtils {
    private static Logger logger = LoggerFactory.getLogger(TimeUtils.class);
    public static final String YEAR = "YEAR";
    public static final String MONTH = "MONTH";
    public static final String DAY = "DAY";
    public static final String HOUR = "HOUR";
    public static final String MIMUE = "MIMUE";
    public static final String SESCOND = "SESCOND";
    public static final String millisecond = "millisecond";
    public static final String WEEK = "WEEK";

    public static String timeStemp2DateStr(String ts, String pattern) {
        Date date = new Date(Long.valueOf(ts));
        SimpleDateFormat df = new SimpleDateFormat(pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
        String formatTime = df.format(date);
        return formatTime;
    }

    public static String date2DateStr(Date date, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
        String formatTime = df.format(date);
        return formatTime;
    }

    public static long dateStr2TimeStemp(String dateStr, String pattern) {
        Date date = null;
        try {
            SimpleDateFormat df = new SimpleDateFormat(pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
            date = df.parse(dateStr);
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
        }
        return date.getTime();
    }

    public static Date dateStr2Date(String dateStr, String pattern) {
        Date date = null;
        try {
            SimpleDateFormat df = new SimpleDateFormat(pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
            date = df.parse(dateStr);
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
        }
        return date;
    }

    /**
     * 计算时间处于当月的第几周
     *
     * @param str 时间格式:"yyyy-MM-dd"
     * @return
     * @throws Exception
     */
    public static int getWEEKOfMoth(String str, String pattern) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern == null ? "yyyy-MM-dd" : pattern);
        Date date = sdf.parse(str);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        //第几周
        int WEEK = calendar.get(Calendar.WEEK_OF_MONTH);
        return WEEK;
    }

    /**
     * 计算时间处于当周的第几天
     *
     * @param str 时间格式:"yyyy-MM-dd"
     * @return
     * @throws Exception
     */
    public static int getDAYOfWEEK(String str, String pattern) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern == null ? "yyyy-MM-dd" : pattern);
        Date date = sdf.parse(str);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        //第几天，从周日开始
        int DAY = calendar.get(Calendar.DAY_OF_WEEK);
        return DAY;
    }

    public static long addTime(String timeStr, String pattern, String addTimeType, int amount) {
        Date date = TimeUtils.dateStr2Date(timeStr, pattern == null ? "yyyyMMddHHmm" : pattern);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        if ("YEAR".equals(addTimeType)) {
            cal.add(Calendar.YEAR, amount);
        } else if ("MONTH".equals(addTimeType)) {
            cal.add(Calendar.MONTH, amount);
        } else if ("DAY".equals(addTimeType)) {
            cal.add(Calendar.DAY_OF_MONTH, amount);
        } else if ("HOUR".equals(addTimeType)) {
            cal.add(Calendar.HOUR_OF_DAY, amount);
        } else if ("MIMUE".equals(addTimeType)) {
            cal.add(Calendar.MINUTE, amount);
        } else if ("SESCOND".equals(addTimeType)) {
            cal.add(Calendar.SECOND, amount);
        } else if ("millisecond".equals(addTimeType)) {
            cal.add(Calendar.MILLISECOND, amount);
        } else if ("WEEK".equals(addTimeType)) {
            cal.add(Calendar.WEEK_OF_MONTH, amount);
        }
        long endTimeInMillis = cal.getTimeInMillis();
        return endTimeInMillis;
    }


//    @Test
    public void test() {
        long ts1 = addTime("20180101000000000", null, this.YEAR, 1);
        long ts2 = addTime("20180101000000000", null, this.MONTH, 1);
        long ts3 = addTime("20180101000000000", null, this.DAY, 1);
        long ts4 = addTime("20180101000000000", null, this.HOUR, 1);
        long ts5 = addTime("20180101000000000", null, this.MIMUE, 1);
        long ts6 = addTime("20180101000000000", null, this.SESCOND, 1);
        long ts7 = addTime("20180101000000000", null, this.millisecond, 1);
        long ts8 = addTime("20180101000000000", null, this.WEEK, 1);
        String s1 = timeStemp2DateStr(ts1 + "", null);
        String s2 = timeStemp2DateStr(ts2 + "", null);
        String s3 = timeStemp2DateStr(ts3 + "", null);
        String s4 = timeStemp2DateStr(ts4 + "", null);
        String s5 = timeStemp2DateStr(ts5 + "", null);
        String s6 = timeStemp2DateStr(ts6 + "", null);
        String s7 = timeStemp2DateStr(ts7 + "", null);
        String s8 = timeStemp2DateStr(ts8 + "", null);
        System.out.println(s1);
        System.out.println(s2);
        System.out.println(s3);
        System.out.println(s4);
        System.out.println(s5);
        System.out.println(s6);
        System.out.println(s7);
        System.out.println(s8);
    }
//    YEAR,MONTH,DAY,HOUR,MIMUE,SESCOND,millisecond,WEEK


}
