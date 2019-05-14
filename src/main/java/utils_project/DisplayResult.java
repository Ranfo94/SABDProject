package utils_project;

import entities.CountryStats;
import entities.Stats;
import entities.YearlyStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static avro.shaded.com.google.common.collect.Iterables.getLast;

public class DisplayResult {

    public static CountryStats printStatsByCountry(Iterable<YearlyStats> humidity, Iterable<YearlyStats> pressure, Iterable<YearlyStats> temperature) {

        YearlyStats last = getLast(humidity);
        System.out.println("\n\b ******* "+ last.getCountry() + " *******");

        CountryStats countryStats = new CountryStats(humidity,pressure,temperature,last.getCountry());
        List<Integer> yearList = countryStats.getListOfYears();
        Collections.sort(yearList);
        System.out.println(yearList);
        int numberOfYears = yearList.size();

        for (int i = 0; i <numberOfYears ; i++) {

            printYearlyStats(countryStats, yearList.get(i));

        }
        return countryStats;

    }

    public static void printYearlyStats(CountryStats countryStats, Integer year) {

        ArrayList<Stats> humidity = countryStats.getYearlyStat("humidity", year).getMonthlyStats();
        ArrayList<Stats> pressure = countryStats.getYearlyStat("pressure", year).getMonthlyStats();
        ArrayList<Stats> temperature = countryStats.getYearlyStat("temperature", year).getMonthlyStats();

        System.out.println("\nYear: "+year);

        Double[] h_mean = new Double[12];
        Double[] h_max = new Double[12];
        Double[] h_min = new Double[12];
        Double[] h_std = new Double[12];

        Double[] p_mean = new Double[12];
        Double[] p_max = new Double[12];
        Double[] p_min = new Double[12];
        Double[] p_std = new Double[12];

        Double[] t_mean = new Double[12];
        Double[] t_max = new Double[12];
        Double[] t_min = new Double[12];
        Double[] t_std = new Double[12];

        String[] months = {"JANUARY","FEBRUARY","MARCH","APRIL","MAY","JUNE","JULY","AUGUST","SEPTEMBER","OCTOBER","NOVEMBER","DECEMBER"};

        for(Stats s : humidity){

            String month = s.getMonth();
            int i = TimeDateManager.getMonthIndex(month)-1;

            h_mean[i]= s.getMean();
            h_max[i]=s.getMax();
            h_min[i]=s.getMin();
            h_std[i]=s.getStd();

        }

        System.out.println("\n--- HUMIDITY --- \n");
        for (int j = 0; j < 12 ; j++) {
            System.out.println(months[j]+"-->  mean:"+h_mean[j]+", max:"+h_max[j]+", min:"+h_min[j]+", std:"+h_std[j]+"");
        }

        for(Stats s : pressure){

            String month = s.getMonth();
            int i = TimeDateManager.getMonthIndex(month)-1;

            p_mean[i]= s.getMean();
            p_max[i]=s.getMax();
            p_min[i]=s.getMin();
            p_std[i]=s.getStd();

        }

        System.out.println("\n--- PRESSURE --- \n");
        for (int j = 0; j < 12 ; j++) {
            System.out.println(months[j]+"-->  mean:"+p_mean[j]+", max:"+p_max[j]+", min:"+p_min[j]+", std:"+p_std[j]+"");
        }

        for(Stats s : temperature){

            String month = s.getMonth();
            int i = TimeDateManager.getMonthIndex(month)-1;

            t_mean[i]= s.getMean();
            t_max[i]=s.getMax();
            t_min[i]=s.getMin();
            t_std[i]=s.getStd();

        }

        System.out.println("\n--- TEMPERATURE --- \n");
        for (int j = 0; j < 12 ; j++) {
            System.out.println(months[j]+"-->  mean:"+t_mean[j]+", max:"+t_max[j]+", min:"+t_min[j]+", std:"+t_std[j]+"");
        }

    }
}
