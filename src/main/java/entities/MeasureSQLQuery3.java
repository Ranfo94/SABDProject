package entities;

import java.io.Serializable;

public class MeasureSQLQuery3 implements Serializable {

    private String city;
    private String year;
    private String month;
    private String day;
    private String hour;
    private String country;
    private Double value;

    public MeasureSQLQuery3(String city, String year, String month, String day, String hour, String country, Double value) {
        this.city = city;
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.country = country;
        this.value = value;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
