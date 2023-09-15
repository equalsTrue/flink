package controller.util;


import controller.common.DateConstant;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

/**
 * 时间工具类.
 *
 * @author magic
 * @data 2022/4/19
 */
public class DateUtil {
    private static final String DF = "yyyy-MM-dd HH:mm:ss";

    private static final String DAY_FORMAT = "yyyy-MM-dd";

    private static final String MINUTES = "yyyy-MM-dd HH:mm";

    private static final String SSS = "yyyy-MM-dd HH:mm:ss.SSS";

    private static final String HH = "yyyy-MM-dd HH";

    /**
     * 将时间转换成yyyy-MM-dd HH:mm:ss的格式字符串.
     *
     * @param time 时间对象
     * @return 格式化后的字符串, 当输入为null时输出为""
     */
    public static String formatYyyyMmDdHhMmSs(Date time) {
        if (time == null) {
            return "";
        }
        try {
            return new SimpleDateFormat(DF).format(time);
        } catch (Exception ex) {
            return "";
        }
    }

    /**
     * long转成String.
     *
     * @param time
     * @return
     */
    public static String formatYyyyMmDdHhMmSs(long time) {
        return new SimpleDateFormat(DF).format(new Date(time));
    }

    /**
     * 将时间转换成yyyy-MM-dd HH的格式字符串.
     *
     * @param time 时间对象
     * @return 格式化后的字符串, 当输入为null时输出为""
     */
    public static String formatYyyyMmDdHh(Date time) {
        if (time == null) {
            return "";
        }
        try {
            return new SimpleDateFormat(HH).format(time);
        } catch (Exception ex) {
            return "";
        }
    }

    /**
     * LONG转成String.
     *
     * @param date
     * @param timeZone
     * @return
     */
    public static String getDateByTimeZone2(long date, String timeZone) {
        SimpleDateFormat format = new SimpleDateFormat(DF);
        format.setTimeZone(TimeZone.getTimeZone(timeZone));
        Date newDate = new Date(date);
        String dateStr = format.format(newDate);
        return dateStr;
    }

    /**
     * string 转成 long.
     *
     * @param timestamp
     * @param timeZone
     * @return
     */
    public static long times2tamptoLong(String timestamp, String timeZone) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(DF);
            sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
            long time = sdf.parse(timestamp).getTime();
            return time;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 获取传入时间,加上number数的时间.
     *
     * @param date
     * @param number
     * @return
     */
    public static String getCurDateBeforeHourStr(Date date, int number) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, -number);
        String time = new SimpleDateFormat(DF).format(calendar.getTime());
        return time;
    }


    /**
     * 指定时间 加上 分/时/天/月.
     * @param dateType
     * @param dBegin
     * @param dEnd
     * @param time
     * @return
     */
    public static List<String> findDates(String dateType, Date dBegin, Date dEnd, int time) {
        try {
            List<String> listDate = new ArrayList<>();
            listDate.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dBegin));
            Calendar calBegin = Calendar.getInstance();
            calBegin.setTime(dBegin);
            Calendar calEnd = Calendar.getInstance();
            calEnd.setTime(dEnd);
            while (calEnd.after(calBegin)) {
                if ("H".equals(dateType)) {
                    calBegin.add(Calendar.HOUR, time);
                }
                if ("M".equals(dateType)) {
                    calBegin.add(Calendar.MONTH, time);
                }
                if ("D".equals(dateType)) {
                    calBegin.add(Calendar.DATE, time);
                }
                if ("N".equals(dateType)) {
                    calBegin.add(Calendar.MINUTE, time);
                }
                if (calEnd.after(calBegin)) {
                    listDate.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calBegin.getTime()));
                }

            }
            return listDate;
        } catch (Exception e) {
            return new ArrayList<>();
        }

    }


    /**
     * LONG转成String.
     *
     * @param date
     * @param timeZone
     * @return
     */
    public static String getDateByTimeZone(long date, String timeZone) {
        SimpleDateFormat format = new SimpleDateFormat(SSS);
        format.setTimeZone(TimeZone.getTimeZone(timeZone));
        Date newDate = new Date(date);
        String dateStr = format.format(newDate);
        return dateStr;
    }

    /**
     * Date转String YyyyMmDdHhMm.
     *
     * @param time
     * @return
     */
    public static String formatYyyyMmDdHhMm(Date time) {
        if (time == null) {
            return "";
        }
        try {
            return new SimpleDateFormat(MINUTES).format(time);
        } catch (Exception ex) {
            return "";
        }
    }

    /**
     * 格式化字符串为日期.
     *
     * @param time 字符串格式为 yyyy-MM-dd HH:mm:ss
     * @return 日期 Date对象
     */
    public static Date formatTime(String time) {
        try {
            return new SimpleDateFormat(DF).parse(time);
        } catch (ParseException e) {
            return new Date();
        }
    }


    /**
     * 给指定时间加上一个数值.
     *
     * @param time1   要加上一数值的时间，为null即为当前时间，格式为yyyy-MM-dd HH:mm:ss
     * @param addpart 要加的部分：年月日时分秒分别为：YMDHFS
     * @param num     要加的数值
     * @return 新时间，格式为yyyy-MM-dd HH:mm:ss
     */
    public static String addTime(String time1, String addpart, int num) {
        try {
            String now = new SimpleDateFormat(DF).format(new Date());
            time1 = (time1 == null) ? now : time1;
            if (time1.length() < DateConstant.DATE_LEN) {
                time1 += " 00:00:00";
            }
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTime(new SimpleDateFormat(DF).parse(time1));
            if (DateConstant.YEAR_STR.equalsIgnoreCase(addpart)) {
                cal.add(Calendar.YEAR, num);
            } else if (DateConstant.MONTH_STR.equalsIgnoreCase(addpart)) {
                cal.add(Calendar.MONTH, num);
            } else if (DateConstant.DAY_STR.equalsIgnoreCase(addpart)) {
                cal.add(Calendar.DATE, num);
            } else if (DateConstant.HOUR_STR.equalsIgnoreCase(addpart)) {
                cal.add(Calendar.HOUR, num);
            } else if (DateConstant.MIN_STR.equalsIgnoreCase(addpart)) {
                cal.add(Calendar.MINUTE, num);
            } else if (DateConstant.SECOND_STR.equalsIgnoreCase(addpart)) {
                cal.add(Calendar.SECOND, num);
            }
            return new SimpleDateFormat(DF).format(cal.getTime());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 格式化字符串为日期.
     *
     * @param time 字符串格式为 yyyy-MM-dd
     * @return 日期 Date对象
     */
    public static Date formatDayTime(String time) {
        try {
            return new SimpleDateFormat(DAY_FORMAT).parse(time);
        } catch (ParseException e) {
            return new Date();
        }
    }

    /**
     * 格式化字符串为日期.
     *
     * @param time 字符串格式为 yyyy-MM-dd
     * @return 日期 Date对象
     */
    public static Date formatDayTimeHh(String time) {
        try {
            return new SimpleDateFormat(HH).parse(time);
        } catch (ParseException e) {
            return new Date();
        }
    }

    /**
     * 将时间转换成yyyy-MM-dd 的格式字符串.
     *
     * @param time 时间对象
     * @return 格式化后的字符串, 当输入为null时输出为""
     */
    public static String formatDay(Date time) {
        if (time == null) {
            return "";
        }
        try {
            return new SimpleDateFormat(DAY_FORMAT).format(time);
        } catch (Exception ex) {
            return "";
        }
    }

    public static String getDayPlusNumber(int number) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, number);
        return new SimpleDateFormat(DAY_FORMAT).format(cal.getTime());
    }

    public static String getDateTimePlusNumber(int number) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, number);
        return new SimpleDateFormat(DF).format(cal.getTime());
    }

    public static String getMinutesPlusNumber(int number) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, number);
        return new SimpleDateFormat(MINUTES).format(cal.getTime());
    }

    /**
     * 两个日期相减，返回差值（单位秒).
     *
     * @param date1 被减数
     * @param date2 减数
     * @return
     */
    public static Long subDate1ToDate2(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            return 0L;
        }
        long result = (date1.getTime() - date2.getTime()) / 1000;
        return result;
    }





    /**
     * 当前日期的前一天.
     *
     * @return 当前日期的前一天
     */
    public static String yesterday() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, -1);
        return new SimpleDateFormat(DAY_FORMAT).format(cal.getTime());
    }

    /**
     * 比较日期(0:day1 与day2相等  -1: day1 小于day2  1：day1 大于day2)
     * @param day1
     * @param day2
     * @return
     */
    public static int compareDay(String day1,String day2){
        int result = 0;
       Date date1 =  formatDayTime(day1);
       Date date2 = formatDayTime(day2);
       if(date1.before(date2)){
            result = -1;
       }else if(date1.after(date2)){
            result = 1;
       }
       return result;
    }



    /**
     * 得到 yyyy-MM-dd 格式的指定日期的前一天.
     */
    public static String beforeDay(String day) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(parseDayByDayFormat(day));
        cal.add(Calendar.DATE, -1);
        return new SimpleDateFormat(DAY_FORMAT).format(cal.getTime());
    }



    /**
     * 得到 yyyy-MM-dd 格式的指定日期的后某天.
     */
    public static String afterDay(String day,int num) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(parseDayByDayFormat(day));
        cal.add(Calendar.DATE, num);
        return new SimpleDateFormat(DAY_FORMAT).format(cal.getTime());
    }

    /**
     * 返回自1970年1月1日00:00:00GMT以来此日期对象表示的毫秒数.
     *
     * @param str 格式为yyyy-MM-dd
     */
    public static long parseDayByDayFormat(String str) {
        try {
            return new SimpleDateFormat(DAY_FORMAT).parse(str).getTime();
        } catch (Exception ex) {
            return 0L;
        }
    }


    /**
     * 返回自1970年1月1日00:00:00GMT以来此日期对象表示的毫秒数.
     *
     * @param str 格式为yyyy-MM-dd
     */
    public static int parseTimeByDayFormat(String str) {
        try {
            long time = new SimpleDateFormat(DF).parse(str).getTime();
            return new BigDecimal(time).divide(new BigDecimal(1000),0, RoundingMode.DOWN).intValue();
        } catch (Exception ex) {
            return 0;
        }
    }


}
