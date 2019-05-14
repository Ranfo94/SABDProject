package entities;

import java.io.Serializable;

public class City implements Serializable {

    private String name;
    private String country;
    private double latitude;
    private double longitute;
    private int timezone_offset;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitute() {
        return longitute;
    }

    public void setLongitute(double longitute) {
        this.longitute = longitute;
    }

    public int getTimezone_offset() {
        return timezone_offset;
    }

    public void setTimezone_offset(int timezone_offset) {
        this.timezone_offset = timezone_offset;
    }

    public City(String name, String country, double latitude, double longitute, int timezone_offset) {
        this.name = name;
        this.country = country;
        this.latitude = latitude;
        this.longitute = longitute;
        this.timezone_offset = timezone_offset;
    }
}
