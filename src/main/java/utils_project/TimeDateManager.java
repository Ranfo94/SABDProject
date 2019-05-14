package utils_project;

import com.skedgo.converter.TimezoneMapper;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class TimeDateManager {

    public static int getMonthIndex(String month) {

        if(month.substring(0,1)=="0"){
            return Integer.parseInt(month.substring(1,2));
        }
        return Integer.parseInt(month);
    }

    public static String getYearMonth(String stringdate){
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dt = formatter.parseLocalDateTime(stringdate);

        if(dt.getMonthOfYear() < 10){
            return  "" + dt.getYear()+"-0"+dt.getMonthOfYear();
        }

        return  "" + dt.getYear()+"-"+dt.getMonthOfYear();
    }

    public static String getYear(String stringdate){
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dt = formatter.parseLocalDateTime(stringdate);
        return  "" + dt.getYear();
    }

    public static String getHour(String stringdate){
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dt = formatter.parseLocalDateTime(stringdate);
        return  "" + dt.getHourOfDay();
    }

    public static String getMonth(String line){

        String stringdate = line.split(",")[0];
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dt = formatter.parseLocalDateTime(stringdate);

        int month = dt.getMonthOfYear();
        if(month <10){
            return "0" + month;
        }

        return String.valueOf(dt.getMonthOfYear());
    }

    public static String getDay(String line){

        String stringdate = line.split(",")[0];
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dt = formatter.parseLocalDateTime(stringdate);

        int day = dt.getDayOfMonth();
        if(day <10){
            return "0" + day;
        }

        return String.valueOf(dt.getDayOfMonth());
    }

    public static String getDate(String stringdate){
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dt = formatter.parseLocalDateTime(stringdate);

        String month="";
        String day="";
        if(dt.getMonthOfYear() < 10){
            month = "0"+dt.getMonthOfYear();
        }else{
            month = ""+dt.getMonthOfYear();
        }

        if(dt.getDayOfMonth() < 10){
            day = "0"+dt.getDayOfMonth();
        }else{
            day = ""+dt.getDayOfMonth();
        }

        return  "" + dt.getYear()+"-"+month+"-"+day;
    }

    public static int getTimeZoneOffset(long latitude, long longitude){

        String timezone = getTimeZone(latitude,longitude);

        ZoneId here = ZoneId.of(timezone);
        ZonedDateTime hereAndNow = Instant.now().atZone(here);

        String offset = hereAndNow.getOffset().toString();

        if(offset.equals("Z")){
            return 0;
        }
        if(offset.startsWith("+")){
            return Integer.parseInt(offset.substring(1,3));
        }
        else{
            return -Integer.parseInt(offset.substring(1,3));
        }

    }

    public static String getTimeZone(long latitude, long longitude){

        return TimezoneMapper.latLngToTimezoneString(latitude,longitude);

    }

    public static int getTimeZoneTime(int hour, int offset){
        int diff= hour+1+offset;
        if(diff >0){
            return diff;
        }else{
            return 23+diff;
        }

    }

    public static String getTimeZoneDate(int offset, int hour, String UTCDate){

        String[] date = UTCDate.split("_");
        String year = date[0];
        String month = date[1];
        String day = date[2];

        int diff = hour + offset;
        if(diff>=0){
            return UTCDate;
        }else{
            Calendar cal = new GregorianCalendar(Integer.parseInt(year),Integer.parseInt(month)-1,Integer.parseInt(day));
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

            cal.add(Calendar.DATE, -1);

            String new_day="";
            String new_year=""+cal.get(Calendar.YEAR);
            String new_month="";

            if(cal.get(Calendar.MONTH)+1 < 10 ){
                new_month = "0"+(cal.get(Calendar.MONTH)+1);
            }else{
                new_month = ""+(cal.get(Calendar.MONTH)+1);
            }

            if(cal.get(Calendar.DATE) < 10 ){
                new_day = "0"+cal.get(Calendar.DATE);
            }else{
                new_day = ""+cal.get(Calendar.DATE);
            }

            String new_date = new_year+"_"+new_month+"_"+new_day;

            return new_date;

        }
    }
}
