package entities;

import java.io.Serializable;

public class WeatherDescriptionSQL implements Serializable {

    private String city;
    private String country;
    private String description;
    private String year;
    private String month;
    private String day;
    private int hour;

    public WeatherDescriptionSQL(String city, String country, String description, String year, String month, String day, int hour) {
        this.city = city;
        this.country = country;
        this.description = description;
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
    }


    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }
}
