package sparkSQL;

import entities.City;
import entities.WeatherDescriptionSQL;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import utils_project.Geolocalizer;
import utils_project.TimeDateManager;

import java.io.IOException;
import java.util.*;

public class Query1PreProcessing {

    private static String pathToDescriptionFile = "dataset/weather_description.csv";
    private static String pathToCityFile = "dataset/city_attributes.csv";

    public static JavaPairRDD<String, WeatherDescriptionSQL> preprocessData(JavaSparkContext sc) throws IOException {

        JavaRDD<String> descriptionFile = sc.textFile(pathToDescriptionFile);

        //TODO: GET FILE FROM HDFS

        JavaRDD<String> cityFile = sc.textFile(pathToCityFile);
        HashMap<String, City> city_countries = Geolocalizer.process_city_location(cityFile);

        String firstRow = descriptionFile.first();
        String[] splittedRow = firstRow.split(",");
        String[] cities = Arrays.copyOfRange(splittedRow,1,splittedRow.length);

        JavaRDD<String> descriptionData = descriptionFile.filter(x -> !x.equals(firstRow));

        //filter out unnecessary months
        JavaRDD<String> dataFilteredByMonth = descriptionData.filter(x -> getMonth(x).equals("03") || getMonth(x).equals("04") || getMonth(x).equals("05"));

        //pairs of (key, weather description)
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

        return weatherDescrJavaRDD;
    }


    private static String getMonth(String s) {

        String date = s.substring(5,7);
        return date;
    }
}
