package entities;

import java.io.Serializable;

public class Measure implements Serializable {

    private double measure;
    private String type;
    private String hour;

    public Measure(double measure, String type,String hour) {
        this.measure = measure;
        this.type = type;
        this.hour = hour;
    }

    public double getMeasure() {
        return measure;
    }

    public void setMeasure(double measure) {
        this.measure = measure;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    @Override
    public String toString() {
        return "Measure{" +
                "measure=" + measure +
                ", type='" + type + '\'' +
                ", hour='" + hour + '\'' +
                '}';
    }
}
