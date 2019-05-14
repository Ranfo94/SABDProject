import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Query: Per ogni anno del dataset
 * individuare le citta` che hanno almeno 15 giorni al mese di tempo sereno nei mesi di marzo, aprile e maggio.
 *
 * file: weather_description.csv
 *
 * output: per ogni anno, elenco delle città
 */

public class Query1 {

    private static String pathToFile = "dataset/weather_description.csv";

    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Inverted index");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> file = sc.textFile(pathToFile);

        long startTime = System.nanoTime();

        //the table header is deleted
        String first_line = file.first();
        JavaRDD<String> data = file.filter(x -> !x.equals(first_line));

        //array of all the cities
        String[] cities_string = first_line.split(",");
        String[] cities = Arrays.copyOfRange(cities_string,1,cities_string.length);

        //all the march/april/may entries are deleted
        JavaRDD<String> dataFilteredByMonth = data.filter(x -> getMonth(x).equals("03") || getMonth(x).equals("04") || getMonth(x).equals("05"));

        //pairs(date + city, weather description)
        JavaPairRDD<String, String> cityHourlyWeather = dataFilteredByMonth.flatMapToPair(new PairFlatMapFunction<String,String,String>(){

            @Override
            public Iterator<Tuple2<String, String>> call(String s) throws Exception {

                List<Tuple2<String, String>> results = new ArrayList<>();

                String[] data = s.split(",");
                String data_string = data[0].split(" ")[0];
                String[] weather_descr = Arrays.copyOfRange(data,1,data.length);

                int i=0;
                for(String desc : weather_descr){

                    Tuple2<String,String> res = new Tuple2<>(data_string+" "+cities[i],desc);
                    results.add(res);

                    i++;
                }

                return results.iterator();
            }

        });

        //pairs grouped by key (date + city, [array of weather descriptions])
        JavaPairRDD<String,Iterable<String>> cityDailyWeather = cityHourlyWeather.groupByKey();

        //check if the day was sunny (year + month + city , 1/0)
        JavaPairRDD<String,Integer> cityMonthlyWeather = cityDailyWeather.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {

                String[] old_key = stringIterableTuple2._1.split(" ");
                String city = stringIterableTuple2._1.substring(11,stringIterableTuple2._1.length());
                String year_month = old_key[0].substring(0,7);
                String new_key = year_month + " " + city;

                Iterable<String> description = stringIterableTuple2._2;
                int res;

//TODO RIVEDERE LA LOGICA DI DECISIONE!

                int i=0;
                for(String s : description){
                    if(s.contains("sky is clear")){
                        i++;
                    }
                    if(s.contains("rain")){
                        i=0;
                        break;
                    }
                }

                int len = Iterables.size(description);
                //60% sky is clear
                int max_percentage = 60*len/100;

                if (i==0){
                    res=0;
                }if(i >= max_percentage){
                    res=1;
                }
                else{
                    res=0;
                }

                Tuple2<String,Integer> result = new Tuple2<>(new_key,res);

                return result;
            }
        });

        //count sunny days per month -> (year + month + city, number of sunny days)
        JavaPairRDD<String, Integer> daysCount = cityMonthlyWeather.reduceByKey((x, y) -> x+y);

        //months whose number of sunny days is over 15 are filtered
        JavaPairRDD<String,Integer> filteredDaysCount = daysCount.filter(x -> x._2 >= 15);

        //pairs (year+city, 1)
        JavaPairRDD<String,Integer> pairsYearCity_One = filteredDaysCount.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {

                String year = stringIntegerTuple2._1.substring(0,4);
                String city = stringIntegerTuple2._1.substring(8,stringIntegerTuple2._1.length());
                String yearCity = year + " " + city;

                Tuple2<String,Integer> result = new Tuple2<>(yearCity,1);
                return result;
            }
        });

        //count months -> (year + city, number of months
        JavaPairRDD<String,Integer> monthsCount = pairsYearCity_One.reduceByKey((x, y) -> x+y);

        //cities whose number of months with over 15 sunny days are filtered
        JavaPairRDD<String,Integer> filteredMonthsCount = monthsCount.filter(x -> x._2 ==3);

        //pairs grouped by city + cut duplicates + grouped by key -> (year, [array of cities])
        JavaPairRDD<String,Iterable<String>> pairsCityYear = filteredMonthsCount.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {

                String year = stringIntegerTuple2._1.substring(0,4);
                String city = stringIntegerTuple2._1.substring(5,stringIntegerTuple2._1.length());

                Tuple2<String,String> result = new Tuple2<>(year,city);

                return result;
            }
        }).distinct().groupByKey();


        //result is printed
        Map<String,Iterable<String>> mapResults = pairsCityYear.collectAsMap();
        for(String year : mapResults.keySet()){

            Iterable<String> cityList = mapResults.get(year);
            System.out.println(year + " - "+cityList+"\n");

        }

        sc.stop();

        //time elapsed

        System.out.println("\n****\n");

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;

        System.out.println("Time Elapsed (nanoseconds) : "+timeElapsed);
        System.out.println("Time Elapsed (milliseconds) : "+timeElapsed/1000000);

    }

//todo: update con funz generica
    private static String getMonth(String s) {

        String date = s.substring(5,7);
        return date;
    }

}
