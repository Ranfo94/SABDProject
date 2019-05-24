package entities;

import java.io.Serializable;

public class CityDiff implements Serializable {

    private String city;
    private String country;
    private Double diff;

    public CityDiff(String city, String country, Double diff) {
        this.city = city;
        this.country = country;
        this.diff = diff;
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

    public Double getDiff() {
        return diff;
    }

    public void setDiff(Double diff) {
        this.diff = diff;
    }
}
