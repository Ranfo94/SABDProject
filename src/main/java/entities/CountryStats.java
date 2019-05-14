package entities;

import entities.YearlyStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CountryStats implements Serializable {

    private Iterable<YearlyStats> yearlyHumidityStats = new ArrayList<>();
    private Iterable<YearlyStats> yearlyPressureStats = new ArrayList<>();
    private Iterable<YearlyStats> yearlyTemperatureStats = new ArrayList<>();

    private String country;

    public List<Integer>  getListOfYears() {

        List<Integer> list = new ArrayList<>();
        for(YearlyStats y : this.yearlyHumidityStats){
                list.add(Integer.parseInt(y.getYear()));
        }
        for(entities.YearlyStats y : this.yearlyPressureStats){
            if(list.contains(Integer.parseInt(y.getYear()))==false){
                list.add(Integer.parseInt(y.getYear()));
            }
        }
        for(entities.YearlyStats y : this.yearlyTemperatureStats){
            if(list.contains(Integer.parseInt(y.getYear()))==false){
                list.add(Integer.parseInt(y.getYear()));
            }
        }
        return list;
    }

    public CountryStats(String country){
        this.country = country;
    }

    public CountryStats(Iterable<YearlyStats> yearlyHumidityStats, Iterable<YearlyStats> yearlyPressureStats, Iterable<YearlyStats> yearlyTemperatureStats, String country) {
        this.yearlyHumidityStats = yearlyHumidityStats;
        this.yearlyPressureStats = yearlyPressureStats;
        this.yearlyTemperatureStats = yearlyTemperatureStats;
        this.country = country;
    }

    public YearlyStats getYearlyStat(String type, Integer year) {


        switch(type){
            case "humidity" :
                return getYearlyStatByYear(this.yearlyHumidityStats,year);

            case "pressure" :
                return getYearlyStatByYear(this.yearlyPressureStats,year);

            case "temperature" :
                return getYearlyStatByYear(this.yearlyTemperatureStats,year);

        }
        return null;

    }

    private YearlyStats getYearlyStatByYear(Iterable<YearlyStats> iterable, Integer year) {

        for(YearlyStats y : iterable){
            if(Integer.parseInt(y.getYear())==year){
                return y;
            }
        }
        return null;
    }


}
