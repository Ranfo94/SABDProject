package entities;

import java.io.Serializable;

public class Stats implements Serializable {

    private double min;
    private double max;
    private double mean;
    private double std;

    String country;
    String year;
    String month;
    String type;

    public Stats(double min, double max, double mean, double std, String country, String year,String month, String type) {
        this.min = min;
        this.max = max;
        this.mean = mean;
        this.std = std;
        this.country = country;
        this.year = year;
        this.type = type;
        this.month = month;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public double getStd() {
        return std;
    }

    public void setStd(double std) {
        this.std = std;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    @Override
    public String toString() {
        return "Stats{" +
                "min=" + min +
                ", max=" + max +
                ", mean=" + mean +
                ", std=" + std +
                ", country='" + country + '\'' +
                ", year='" + year + '\'' +
                ", month='" + month + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
