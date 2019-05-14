package sparkSQL;

import entities.City;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import utils_project.Geolocalizer;
import utils_project.TimeDateManager;

import java.io.IOException;
import java.util.*;

import static avro.shaded.com.google.common.collect.Iterables.size;

public class Query1PreProcessing {

    private static String pathToDescriptionFile = "dataset/weather_description.csv";
    private static String pathToCityFile = "dataset/city_attributes.csv";

    public static JavaPairRDD<String,Iterable<Integer>> preprocessData(JavaSparkContext sc) throws IOException {

        JavaRDD<String> descriptionFile = sc.textFile(pathToDescriptionFile);

        JavaRDD<String> cityFile = sc.textFile(pathToCityFile);
        HashMap<String, City> city_countries = process_city_location(cityFile);

        String firstRow = descriptionFile.first();
        String[] splittedRow = firstRow.split(",");
        String[] cities = Arrays.copyOfRange(splittedRow,1,splittedRow.length);

        JavaRDD<String> descriptionData = descriptionFile.filter(x -> !x.equals(firstRow));

        JavaRDD<String> dataFilteredByMonth = descriptionData.filter(x -> getMonth(x).equals("03") || getMonth(x).equals("04") || getMonth(x).equals("05"));

        JavaPairRDD<String,WeatherDescriptionSQL> weatherDescrJavaRDD = dataFilteredByMonth.flatMapToPair(new PairFlatMapFunction<String, String, WeatherDescriptionSQL>() {
            @Override
            public Iterator<Tuple2<String, WeatherDescriptionSQL>> call(String s) throws Exception {

                List<Tuple2<String, WeatherDescriptionSQL>> results = new ArrayList<>();

                String[] line = s.split(",");
                String[] dateTime = line[0].split(" ");
                String[] date = dateTime[0].split("-");

                String year = date[0];
                String month = date[1];
                String day = date[2];

                int hour = Integer.parseInt(dateTime[1].split(":")[0]);

                for (int i = 1; i < line.length ; i++) {
                    //create new WD object

                    String cityName = cities[i-1];
                    City city = city_countries.get(cityName);

                    int offset = city.getTimezone_offset();
                    int new_hour= TimeDateManager.getTimeZoneTime(hour,offset);

                    String[] new_date = TimeDateManager.getTimeZoneDate(offset,hour,year+"_"+month+"_"+day).split("_");

                    WeatherDescriptionSQL wd = new WeatherDescriptionSQL(city.getName(),city.getCountry(),line[i],new_date[0],new_date[1],new_date[2],new_hour);

                    //chiave: (city_day_month_year)
                    String key = cityName +"_"+new_date[2]+"_"+new_date[1]+"_"+new_date[0];

                    results.add(new Tuple2<>(key,wd));
                }
                return results.iterator();

            }

        });

        //(city_day_month_year, [descr ... ]
        JavaPairRDD<String,Iterable<WeatherDescriptionSQL>> wdGroupedByDay = weatherDescrJavaRDD.groupByKey();

        //(city_month_year, 1/0)
        JavaPairRDD<String, Integer> filteredDaysGroupedByDay = wdGroupedByDay.mapToPair(new PairFunction<Tuple2<String, Iterable<WeatherDescriptionSQL>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<WeatherDescriptionSQL>> stringIterableTuple2) throws Exception {

                int sunny =0;

                for(WeatherDescriptionSQL wd : stringIterableTuple2._2){

                    if(wd.getDescription().contains("sky is clear")){
                        sunny++;
                    }

                }
                int res =0;
                if(sunny >= size(stringIterableTuple2._2)*60/100){
                    res =1;
                }
                String[] old_key = stringIterableTuple2._1.split("_");

                //city_month_year
                String new_key= old_key[0]+"_"+old_key[2]+"_"+old_key[3];

                return new Tuple2<>(new_key,res);
            }
        });

        //(city_month_year, [1,0, ...]
        JavaPairRDD<String,Iterable<Integer>> filteredDaysByMonth = filteredDaysGroupedByDay.groupByKey();

        return filteredDaysByMonth;
    }

    private static HashMap<String, City> process_city_location(JavaRDD<String> cityData) throws IOException {
        //todo: arraylist of cities;

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

    private static String getMonth(String s) {

        String date = s.substring(5,7);
        return date;
    }

    public static List<String> getYearList(JavaPairRDD<String,Iterable<Integer>> rdd){
        Map<String,Iterable<Integer>> mappedData = rdd.collectAsMap();
        List<String> yearList = new ArrayList<>();

        for(String k : mappedData.keySet()){
            String year = k.split("_")[2];
            if(yearList.contains(year)==false){
                yearList.add(year);
            }
        }
        return yearList;
    }
}
