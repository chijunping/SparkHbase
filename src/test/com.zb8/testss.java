package com.zb8;


import com.zb8.utils.PhoenixJDBCUtil;
import com.zb8.utils.TimeUtils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class testss {
    public static void main(String[] args) {
        System.out.println("world ");
        List<String> newTime = getNewTime();
        System.out.println(newTime);
    }

    static String phoenixJdbcUrl = "jdbc:phoenix:hb-proxy-pub-bp151dhf9a35tg4f4-002.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-001.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-003.hbase.rds.aliyuncs.com:2181";

    private static List<String> getNewTime() {
        String sql_maxEndTime = "SELECT  ENDTIME \"maxEndTime\" FROM ZB8_STAT_LABEL ORDER BY ENDTIME DESC LIMIT 1";
        PhoenixJDBCUtil.setPhoenixJDBCUrl(phoenixJdbcUrl);
        String newStartTime = PhoenixJDBCUtil.queryForSingleColumIgnoreCase(sql_maxEndTime, null);
        if (newStartTime == null) {
            //newStartTime = TimeUtils.date2DateStr(new Date(), "yyyyMMddHHmm");
            newStartTime = "201809141020";
        }
        Date newStartTimeDate = TimeUtils.dateStr2Date(newStartTime, "yyyyMMddHHmm");
        Calendar cal = Calendar.getInstance();
        cal.setTime(newStartTimeDate);
        cal.add(Calendar.MINUTE, 10);
        long endTimeInMillis = cal.getTimeInMillis();
        if ((System.currentTimeMillis() - endTimeInMillis) < 0) {
            return null;
        }
        String newEndTime = TimeUtils.timeStemp2DateStr(String.valueOf(cal.getTimeInMillis()), "yyyyMMddHHmm");
        return Arrays.asList(newStartTime, newEndTime);
    }
}
