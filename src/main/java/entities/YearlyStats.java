package entities;

import java.io.Serializable;
import java.util.ArrayList;

public class YearlyStats implements Serializable {

    private ArrayList<Stats> monthlyStats = new ArrayList<>();

    String year;
    String country;
    String type;



    public Stats getStatsByMonth(String month){
        for(Stats s : this.monthlyStats){
            if(s.getMonth().equals(month)){
                return s;
            }
        }
        return null;
    }

    public ArrayList<Stats> getMonthlyStats() {
        return monthlyStats;
    }

    public String getYear() {
        return year;
    }

    public String getCountry() {
        return country;
    }

    public String getType() {
        return type;
    }

    public YearlyStats(String year, String country, String type) {
        this.year = year;
        this.country = country;
        this.type = type;

    }

    public void addMonth(Stats month){
        monthlyStats.add(month);
    }
}
