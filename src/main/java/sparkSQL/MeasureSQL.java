package sparkSQL;

import java.io.Serializable;

public class MeasureSQL implements Serializable {

    private String city;
    private String country;
    private String type;
    private Double value;

    public MeasureSQL(String city, String country, String type, Double value) {
        this.city = city;
        this.country = country;
        this.type = type;
        this.value = value;
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
