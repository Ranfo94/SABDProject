package sparkSQL;

import entities.City;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import utils_project.Geolocalizer;
import utils_project.TimeDateManager;

import java.io.IOException;
import java.util.*;

public class Query2PreProcessing {

    private static String pathToHumidityFile = "dataset/humidity.csv";
    private static String pathToTemperatureFile = "dataset/temperature.csv";
    private static String pathToPressureFile = "dataset/pressure.csv";

    private static String pathToCityFile = "dataset/city_attributes.csv";

    public static HashMap<String, City> getCountriesList(JavaSparkContext sc) throws IOException {

        JavaRDD<String> cityFile = sc.textFile(pathToCityFile);
        HashMap<String, City> city_countries = process_city_location(cityFile);
        return city_countries;
    }

    public static List<JavaPairRDD<String,MeasureSQL>> preprocessData(JavaSparkContext sc, HashMap<String,City>city_countries) throws IOException {

        JavaRDD<String> temperatureFile = sc.textFile(pathToTemperatureFile);
        JavaRDD<String> humidityFile = sc.textFile(pathToHumidityFile);
        JavaRDD<String> pressureFile = sc.textFile(pathToPressureFile);

        String firstRow = temperatureFile.first();
        String[] splittedRow = firstRow.split(",");
        String[] cities = Arrays.copyOfRange(splittedRow,1,splittedRow.length);

        JavaRDD<String> temperatureData = temperatureFile.filter(x -> !x.equals(firstRow));
        JavaRDD<String> humidityData = humidityFile.filter(x -> !x.equals(firstRow));
        JavaRDD<String> pressureData = pressureFile.filter(x -> !x.equals(firstRow));

        /**
         * outout (year_month_day_hour, measure_SQL)
         */
        JavaPairRDD<String,MeasureSQL> temperatureValues = temperatureData.flatMapToPair(new PairFlatMapFunction<String, String, MeasureSQL>() {
            @Override
            public Iterator<Tuple2<String, MeasureSQL>> call(String s) throws Exception {

                List<Tuple2<String,MeasureSQL>> results = new ArrayList<>();

                String[] line = s.split(",");

                String[] key = line[0].split(" ");
                String[] date = key[0].split("-");
                // (year_month_day_hour)
                String new_key = date[0]+"_"+date[1]+"_"+date[2]+"_"+key[1].split("-")[0];

                for (int i = 1; i < line.length; i++) {

                    Double v;
                    if(line[i].equals("") || line[i].equals(" ")){
                        v=0.0;
                    }else{
                        v = Double.parseDouble(line[i]);
                    }

                    MeasureSQL m = new MeasureSQL(cities[i-1],city_countries.get(cities[i-1]).getCountry(),"temperature",v);
                    results.add(new Tuple2<>(new_key,m));

                }
                return results.iterator();
            }
        });

        /**
         * outout (year_month_day_hour, measure_SQL)
         */
        JavaPairRDD<String,MeasureSQL> humidityValues = humidityData.flatMapToPair(new PairFlatMapFunction<String, String, MeasureSQL>() {
            @Override
            public Iterator<Tuple2<String, MeasureSQL>> call(String s) throws Exception {

                List<Tuple2<String,MeasureSQL>> results = new ArrayList<>();

                String[] line = s.split(",");

                String[] key = line[0].split(" ");
                String[] date = key[0].split("-");
                // (year_month_day_hour)
                String new_key = date[0]+"_"+date[1]+"_"+date[2]+"_"+key[1].split("-")[0];

                for (int i = 1; i < line.length; i++) {

                    Double v;
                    if(line[i].equals("") || line[i].equals(" ")){
                        v=0.0;
                    }else{
                        v = Double.parseDouble(line[i]);
                    }

                    MeasureSQL m = new MeasureSQL(cities[i-1],city_countries.get(cities[i-1]).getCountry(),"humidity",v);
                    results.add(new Tuple2<>(new_key,m));

                }
                return results.iterator();
            }
        });

        /**
         * outout (year_month_day_hour, measure_SQL)
         */
        JavaPairRDD<String,MeasureSQL> pressureValues = pressureData.flatMapToPair(new PairFlatMapFunction<String, String, MeasureSQL>() {
            @Override
            public Iterator<Tuple2<String, MeasureSQL>> call(String s) throws Exception {

                List<Tuple2<String,MeasureSQL>> results = new ArrayList<>();

                String[] line = s.split(",");

                String[] key = line[0].split(" ");
                String[] date = key[0].split("-");
                // (year_month_day_hour)
                String new_key = date[0]+"_"+date[1]+"_"+date[2]+"_"+key[1].split("-")[0];

                for (int i = 1; i < line.length; i++) {

                    Double v;
                    if(line[i].equals("") || line[i].equals(" ")){
                        v=0.0;
                    }else{
                        v = Double.parseDouble(line[i]);
                    }

                    MeasureSQL m = new MeasureSQL(cities[i-1],city_countries.get(cities[i-1]).getCountry(),"pressure",v);
                    results.add(new Tuple2<>(new_key,m));

                }
                return results.iterator();
            }
        });

        List<String> countries = new ArrayList<>();
        for(String k : city_countries.keySet()){
            String c = city_countries.get(k).getCountry();
            if(countries.contains(c)==false){
                countries.add(c);
            }
        }

        List<JavaPairRDD<String,MeasureSQL>> list = new ArrayList<>();
        list.add(temperatureValues);
        list.add(humidityValues);
        list.add(pressureValues);

        return list;

    }

    private static HashMap<String, City> process_city_location(JavaRDD<String> cityData) throws IOException {

        HashMap<String,City> countrymap = new HashMap<>();
        String firstRow = cityData.first();
        JavaRDD<String> withoutfirst = cityData.filter(x -> !x.equals(firstRow));

        for (String line : withoutfirst.collect()){
            String[] splittedLine = line.split(",");
            long lat = (long) Double.parseDouble(splittedLine[1]);
            long lon = (long) Double.parseDouble(splittedLine[2]);

            String country = new Geolocalizer().localize(splittedLine[1],splittedLine[2]);
            int offset = TimeDateManager.getTimeZoneOffset(lat,lon);
            City city = new City(splittedLine[0],country,lat,lon,offset);
            countrymap.put(splittedLine[0],city);
        }
        return countrymap;
    }


}
