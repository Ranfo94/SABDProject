package spark;

import entities.City;
import entities.CityDiff;
import entities.MeasureSQLQuery3;
import org.apache.spark.SparkConf;
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

public class Query3 {

    private static String pathToTempFile = "cleaned_dataset/cleaned_temperature.csv";
    private static String pathToCityFile = "dataset/city_attributes.csv";

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Inverted index");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long startTime = System.nanoTime();

        //TODO: GET FILE FROM HDFS

        JavaRDD<String> rawData = sc.textFile(pathToCityFile);
        HashMap<String, City> city_countries = Geolocalizer.process_city_location(rawData);

        //get temp data
        JavaRDD<String> tempRawData = sc.textFile(pathToTempFile);

        //cities
        String firstRow = tempRawData.first();
        String[] cities = firstRow.split(",");

        JavaRDD<String> tempData = tempRawData.filter(x-> !x.equals(firstRow));

        JavaPairRDD<String, MeasureSQLQuery3> pairsKeyMeasure = tempData.flatMapToPair(new PairFlatMapFunction<String, String, MeasureSQLQuery3>() {
            @Override
            public Iterator<Tuple2<String, MeasureSQLQuery3>> call(String line) throws Exception {

                List<Tuple2<String, MeasureSQLQuery3>> results = new ArrayList<>();

                String[] tempValues = line.split(",");

                for (int i = 1; i < tempValues.length; i++) {

                    if(tempValues[i].equals("")||tempValues[i].equals(" ")){
                        continue;
                    }

                    String year =  TimeDateManager.getYear(tempValues[0]);
                    String month = TimeDateManager.getMonth(tempValues[0]);
                    String day = TimeDateManager.getDay(tempValues[0]);
                    int hour = Integer.parseInt(TimeDateManager.getHour(tempValues[0]));

                    int offset = city_countries.get(cities[i]).getTimezone_offset();

                    String[] new_date = TimeDateManager.getTimeZoneDate(offset,hour,year+"_"+month+"_"+day).split("_");

                    String new_year =  new_date[0];
                    String new_month = new_date[1];
                    String new_day = new_date[2];

                    String new_hour = TimeDateManager.getTimeZoneTime(hour,offset)+"";

                    String city = city_countries.get(cities[i]).getName();
                    String country = city_countries.get(cities[i]).getCountry();

                    MeasureSQLQuery3 m = new MeasureSQLQuery3(city,new_year,new_month,new_day,new_hour,country,Double.parseDouble(tempValues[i]));

                    String quarter;
                    if(m.getMonth().equals("01") || m.getMonth().equals("02") || m.getMonth().equals("03") || m.getMonth().equals("04")){
                        quarter="First";
                    }else{
                        quarter="Second";
                    }
                    String key = country+"_"+city+"_"+quarter;

                    results.add(new Tuple2<>(key,m));
                }

                return results.iterator();
            }
        });

        //filter out data by month and year

        JavaPairRDD<String,MeasureSQLQuery3> dataFilteredByMonth = pairsKeyMeasure.filter(x -> x._2.getMonth().equals("01") || x._2.getMonth().equals("02") ||
                x._2.getMonth().equals("03") || x._2.getMonth().equals("04") || x._2.getMonth().equals("06") ||
                x._2.getMonth().equals("07") || x._2.getMonth().equals("08") || x._2.getMonth().equals("09"));

        JavaPairRDD<String,MeasureSQLQuery3> dataFilteredByHour = dataFilteredByMonth.filter(x -> x._2.getHour().equals("12") ||x._2.getHour().equals("13") ||
                x._2.getHour().equals("14") || x._2.getHour().equals("15"));

        JavaPairRDD<String,MeasureSQLQuery3> data2k17 = dataFilteredByHour.filter(x -> x._2.getYear().equals("2017"));
        JavaPairRDD<String,MeasureSQLQuery3> data2k16 = dataFilteredByHour.filter(x -> x._2.getYear().equals("2016"));

        //output: country_city_quarter, [measures..]
        JavaPairRDD<String,Iterable<MeasureSQLQuery3>> rdd1k17ByCityQuarter = data2k17.groupByKey();
        JavaPairRDD<String,Iterable<MeasureSQLQuery3>> rdd1k16ByCityQuarter = data2k16.groupByKey();

        //output: country_city_quarter, mean)
        JavaPairRDD<String,Iterable<Double>> rdd2k17Mean = rdd1k17ByCityQuarter.mapToPair(new getQuarterMean()).groupByKey();
        JavaPairRDD<String,Iterable<Double>> rdd2k16Mean = rdd1k16ByCityQuarter.mapToPair(new getQuarterMean()).groupByKey();

        //output: country, [city + diff] array
        JavaPairRDD<String, Iterable<CityDiff>> rdd2k17Diff = rdd2k17Mean.mapToPair(new meansToDiff()).groupByKey();
        JavaPairRDD<String, Iterable<CityDiff>> rdd2k16Diff = rdd2k16Mean.mapToPair(new meansToDiff()).groupByKey();

        Map<String,Iterable<CityDiff>> map2k17 = rdd2k17Diff.collectAsMap();
        Map<String,Iterable<CityDiff>> map2k16 = rdd2k16Diff.collectAsMap();

        long endTime = System.nanoTime();

        for(String k : map2k17.keySet()){

            List<Tuple2<String, Integer>> res = getResultByNation(map2k17.get(k), map2k16.get(k));
            //TODO: SAVE FILE
            System.out.println(res);

        }

        System.out.println("\n****\n");

        long timeElapsed = endTime - startTime;

        System.out.println("Time Elapsed (nanoseconds) : " + timeElapsed);
        System.out.println("Time Elapsed (milliseconds) : " + timeElapsed / 1000000);

    }

    private static List<Tuple2<String, Integer>> getResultByNation(Iterable<CityDiff> list2017, Iterable<CityDiff> list2016) {

        List<String> citiesRank2017 = getCityRank(list2017);
        List<String> citiesRank2016 = getCityRank(list2016);

        List<Tuple2<String, Integer>> result = new ArrayList<>();

        for (int i = 0; i < 3; i++) {

            String name = citiesRank2017.get(i);
            Integer index = citiesRank2016.indexOf(name);

            result.add(new Tuple2<>(name, index+1));


        }

        return result;

    }

    private static List<String> getCityRank(Iterable<CityDiff> list2017) {

        List<CityDiff> list = new ArrayList<>();
        for(CityDiff cd : list2017){

            list.add(cd);

        }

        List<String> ranking = new ArrayList<>();
        int dimen = list.size();

        for (int i = 0; i < dimen; i++) {

            CityDiff max = getMaxFromList(list);
            list.remove(max);
            ranking.add(max.getCity());
        }

        return ranking;

    }

    private static CityDiff getMaxFromList(List<CityDiff> list) {

        Double value=0.0;
        CityDiff maxCity = null;
        for(CityDiff cd : list){
            if(cd.getDiff()> value){
                value=cd.getDiff();
                maxCity=cd;
            }
        }
        return maxCity;
    }


    private static Double getMean(List<Double> valuesList) {

        double m = 0.0;
        for(Double d : valuesList){
            m += d;
        }
        return m/size(valuesList);
    }

    private static class meansToDiff implements PairFunction<Tuple2<String, Iterable<Double>>, String, CityDiff> {
        @Override
        public Tuple2<String, CityDiff> call(Tuple2<String, Iterable<Double>> stringIterableTuple2) throws Exception {
            List<Double> list = new ArrayList<>();
            for(Double d : stringIterableTuple2._2){
                list.add(d);
            }
            Double diff = Math.abs(list.get(0)- list.get(1));
            String[] key = stringIterableTuple2._1.split("_");
            String new_key = key[0];
            CityDiff cd = new CityDiff(key[1],key[0],diff);

            return new Tuple2<>(new_key,cd);
        }
    }

    private static class getQuarterMean implements PairFunction<Tuple2<String, Iterable<MeasureSQLQuery3>>, String, Double> {
        @Override
        public Tuple2<String, Double> call(Tuple2<String, Iterable<MeasureSQLQuery3>> stringIterableTuple2) throws Exception {

            Iterable<MeasureSQLQuery3> measures = stringIterableTuple2._2;
            List<Double> valuesList = new ArrayList<>();

            for(MeasureSQLQuery3 m : measures){
                valuesList.add(m.getValue());
            }

            Double mean = getMean(valuesList);

            String[] old_key = stringIterableTuple2._1.split("_");
            //key country + city
            String new_key = old_key[0]+"_"+old_key[1];

            return new Tuple2<>(new_key,mean);
        }
    }

}
