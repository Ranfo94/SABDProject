import entities.City;
import entities.Measure;
import entities.Stats;
import entities.YearlyStats;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import entities.CountryStats;
import utils_project.DisplayResult;
import utils_project.Geolocalizer;
import utils_project.TimeDateManager;

import java.io.IOException;
import java.util.*;

import static avro.shaded.com.google.common.collect.Iterables.getLast;
import static avro.shaded.com.google.common.collect.Iterables.size;

public class Query2 {

 //   private static String pathToPressureFile = "dataset/pressure.csv";
//    private static String pathToHumidityFile = "dataset/humidity.csv";
//    private static String pathToTemperatureFile = "dataset/temperature.csv";

    public static String pathToPressureFile = "cleaned_dataset/cleaned_pressure.csv";
    public static String pathToHumidityFile = "cleaned_dataset/cleaned_humidity.csv";
    public static String pathToTemperatureFile = "cleaned_dataset/cleaned_temperature.csv";

    private static String pathToCityFile = "dataset/city_attributes.csv";

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Weather Conditions");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long startTime = System.nanoTime();

        JavaRDD<String> rawData = sc.textFile(pathToCityFile);
        HashMap<String, City> city_countries = Geolocalizer.process_city_location(rawData);

        //get humidity data
        JavaRDD<String> humidityRawData = sc.textFile(pathToHumidityFile);
        //get pressure data
        JavaRDD<String> pressureRawData = sc.textFile(pathToPressureFile);
        //get temperature data
        JavaRDD<String> temperatureRawData = sc.textFile(pathToTemperatureFile);

        //cities
        String firstRow = humidityRawData.first();
        String[] cities = firstRow.split(",");


        JavaRDD<String> humidityData = humidityRawData.filter(x -> !x.equals(firstRow));
        JavaRDD<String> pressureData = pressureRawData.filter(x -> !x.equals(firstRow));
        JavaRDD<String> temperatureData = temperatureRawData.filter(x -> !x.equals(firstRow));

        /**
         * output: pairs(key: country_year_month_day, [measure, measure, ...])
         */
        JavaPairRDD<String, Iterable<Measure>> humidityByCountryDate = humidityData.flatMapToPair(new PairFlatMapFunction<String, String, Measure>() {
            @Override
            public Iterator<Tuple2<String, Measure>> call(String line) throws Exception {

                List<Tuple2<String,Measure>> results = new ArrayList<>();
                String[] humidityValues = line.split(",");

                for (int j = 1; j < humidityValues.length; j++) {

                    if(humidityValues[j].equals("")||humidityValues[j].equals(" ")){
                        continue;
                    }

                    String year =  TimeDateManager.getYear(humidityValues[0]);
                    String month = TimeDateManager.getMonth(humidityValues[0]);
                    String day = TimeDateManager.getDay(humidityValues[0]);
                    int hour = Integer.parseInt(TimeDateManager.getHour(humidityValues[0]));

                    int offset = city_countries.get(cities[j]).getTimezone_offset();

                    String[] new_date = TimeDateManager.getTimeZoneDate(offset,hour,year+"_"+month+"_"+day).split("_");

                    String new_year =  new_date[0];
                    String new_month = new_date[1];
                    String new_day = new_date[2];

                   String new_hour = TimeDateManager.getTimeZoneTime(hour,offset)+"";

                    //key: country_year_month_day
                    String key = city_countries.get(cities[j]).getCountry()+"_"+ new_date[0] +"_"+ new_date[1] +"_"+new_date[2];



                    Measure m = new Measure(Double.parseDouble(humidityValues[j]),"humidity",""+new_hour);
                    results.add(new Tuple2<>(key,m));

                }

                return results.iterator();
            }
        }).groupByKey();

        /**
         * output: (country_city_year_month_day, [measure, ..]);
         */
        JavaPairRDD<String, Iterable<Measure>> temperatureByCountryDate = temperatureData.flatMapToPair(new PairFlatMapFunction<String, String, Measure>() {
            @Override
            public Iterator<Tuple2<String, Measure>> call(String line) throws Exception {

                List<Tuple2<String,Measure>> results = new ArrayList<>();

                String[] temperatureValues = line.split(",");

                for (int j = 1; j < temperatureValues.length; j++) {

                    if(temperatureValues[j].equals("")||temperatureValues[j].equals(" ")){
                        continue;
                    }

                    String year =  TimeDateManager.getYear(temperatureValues[0]);
                    String month = TimeDateManager.getMonth(temperatureValues[0]);
                    String day = TimeDateManager.getDay(temperatureValues[0]);
                    int hour = Integer.parseInt(TimeDateManager.getHour(temperatureValues[0]));

                    int offset = city_countries.get(cities[j]).getTimezone_offset();

                    String[] new_date = TimeDateManager.getTimeZoneDate(offset,hour,year+"_"+month+"_"+day).split("_");

                    String new_year =  new_date[0];
                    String new_month = new_date[1];
                    String new_day = new_date[2];

                    String new_hour = TimeDateManager.getTimeZoneTime(hour,offset)+"";

                    //key: country_year_month_day
                    String key = city_countries.get(cities[j]).getCountry()+"_"+ new_date[0]+"_"+new_date[1]+"_"+new_date[2];

                    Measure m = new Measure(Double.parseDouble(temperatureValues[j]),"temperature",""+new_hour);
                    results.add(new Tuple2<>(key,m));
                }

                return results.iterator();
            }
        }).groupByKey();

        /**
         * output: (country_city_year_month_day, [measure, ..]);
         */
        JavaPairRDD<String, Iterable<Measure>> pressureByCountryDate = pressureData.flatMapToPair(new PairFlatMapFunction<String, String, Measure>() {
            @Override
            public Iterator<Tuple2<String, Measure>> call(String line) throws Exception {

                List<Tuple2<String,Measure>> results = new ArrayList<>();

                String[] pressureValues = line.split(",");

                for (int j = 1; j < pressureValues.length; j++) {

                    if(pressureValues[j].equals("")||pressureValues[j].equals(" ")){
                        continue;
                    }

                    String year =  TimeDateManager.getYear(pressureValues[0]);
                    String month = TimeDateManager.getMonth(pressureValues[0]);
                    String day = TimeDateManager.getDay(pressureValues[0]);
                    int hour = Integer.parseInt(TimeDateManager.getHour(pressureValues[0]));

                    int offset = city_countries.get(cities[j]).getTimezone_offset();

                    String[] new_date = TimeDateManager.getTimeZoneDate(offset,hour,year+"_"+month+"_"+day).split("_");

                    String new_year =  new_date[0];
                    String new_month = new_date[1];
                    String new_day = new_date[2];

                    String new_hour = TimeDateManager.getTimeZoneTime(hour,offset)+"";

                    //key: country_year_month_day
                    String key = city_countries.get(cities[j]).getCountry()+"_"+ new_year+"_"+new_month+"_"+new_day;

                    Measure m = new Measure(Double.parseDouble(pressureValues[j]),"pressure",""+new_hour);
                    results.add(new Tuple2<>(key,m));
                }

                return results.iterator();
            }
        }).groupByKey();

        /**
         * in questa funzione aggiusto i dati mancanti e aggrego per country_year_month
         * output= (country_year_month, ArrayList[measures...])
         */
        JavaPairRDD<String, Iterable<ArrayList<Measure>>> dailyHumidityMeasuresByYearCountry = humidityByCountryDate.mapToPair(new collectMeasures()).groupByKey();
        JavaPairRDD<String, Iterable<ArrayList<Measure>>> dailyPressureMeasuresByYearCountry = pressureByCountryDate.mapToPair(new collectMeasures()).groupByKey();
        JavaPairRDD<String, Iterable<ArrayList<Measure>>> dailyTemperatureMeasuresByYearCountry = temperatureByCountryDate.mapToPair(new collectMeasures()).groupByKey();

        /**
         * input: (country_year_month, [measures..])
         * output: (country_year, Stats);
         */
        JavaPairRDD<String, Iterable<Stats>> monthlyHumidityStatsByCountry = dailyHumidityMeasuresByYearCountry.mapToPair(new computeStatsByMonth()).groupByKey();
        JavaPairRDD<String, Iterable<Stats>> monthlyPressureStatsByCountry = dailyPressureMeasuresByYearCountry.mapToPair(new computeStatsByMonth()).groupByKey();
        JavaPairRDD<String, Iterable<Stats>> monthlyTemperatureStatsByCountry = dailyTemperatureMeasuresByYearCountry.mapToPair(new computeStatsByMonth()).groupByKey();

        /**
         * funzione che aggrega per country e crea un oggetto Yearly Stats
         * output: (country, [Yearly Stats])
         */
        Map<String,Iterable<YearlyStats>> yearlyHumidityStatsByCountry = monthlyHumidityStatsByCountry.mapToPair(new computeYearlyStats()).groupByKey().collectAsMap();
        Map<String,Iterable<YearlyStats>> yearlyPressureStatsByCountry = monthlyPressureStatsByCountry.mapToPair(new computeYearlyStats()).groupByKey().collectAsMap();
        Map<String,Iterable<YearlyStats>> yearlyTemperatureStatsByCountry = monthlyTemperatureStatsByCountry.mapToPair(new computeYearlyStats()).groupByKey().collectAsMap();

        /**
         * funzione che aggrega i dati in classe Country Stats
         */
        List<CountryStats> finalResult = collectDataByCountry(yearlyHumidityStatsByCountry,yearlyPressureStatsByCountry,yearlyTemperatureStatsByCountry);


        sc.stop();

        System.out.println("\n****\n");

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;

        System.out.println("Time Elapsed (nanoseconds) : " + timeElapsed);
        System.out.println("Time Elapsed (milliseconds) : " + timeElapsed / 1000000);


    }
    private static List<CountryStats> collectDataByCountry(Map<String,Iterable<YearlyStats>> humidity, Map<String,Iterable<YearlyStats>> pressure, Map<String,Iterable<YearlyStats>> temperature) {

        List<CountryStats> finalResult = new ArrayList<>();
        for(String s : humidity.keySet()){
            finalResult.add(DisplayResult.printStatsByCountry(humidity.get(s),pressure.get(s),temperature.get(s)));
        }

        return finalResult;
    }


    /**
     * fix data + change key
     * output (country_year_month, [measures ...])
     */
    private static class collectMeasures implements PairFunction<Tuple2<String,Iterable<Measure>>,String,ArrayList<Measure>> {

        @Override
        public Tuple2<String, ArrayList<Measure>> call(Tuple2<String, Iterable<Measure>> stringArrayListTuple2) throws Exception {

            String[] key = stringArrayListTuple2._1.split("_");
            String new_key = key[0]+"_"+key[1]+"_"+key[2];

            ArrayList<Measure> measureList = new ArrayList<>();
            for (Measure m : stringArrayListTuple2._2){
                measureList.add(m);
            }

            return new Tuple2<>(new_key,measureList);
        }
    }

    /**
     * input: (country_year_month,Stats);
     * output: (country_year, Stats);
     */
    private static class computeStatsByMonth implements PairFunction<Tuple2<String, Iterable<ArrayList<Measure>>>, String, Stats> {
        @Override
        public Tuple2<String, Stats> call(Tuple2<String, Iterable<ArrayList<Measure>>> stringIterableTuple2) throws Exception {

            String[] key = stringIterableTuple2._1.split("_");

            //country_year
            String new_key = key[0]+"_"+key[1];

            Iterable<ArrayList<Measure>> measures = stringIterableTuple2._2;

            ArrayList<Measure> measuresList = new ArrayList<>();

            for(ArrayList<Measure> a : measures){
                measuresList.addAll(a);
            }

            int len = size(measures);

            String type = measuresList.get(0).getType();

            List<Double> valuesList = new ArrayList<>();

            for(Measure m : measuresList){
                valuesList.add(m.getMeasure());
            }

            Collections.sort(valuesList);
            double max = valuesList.get(valuesList.size() - 1);
            double min = valuesList.get(0);
            double mean = getMean(valuesList);
            double std = getSTD(valuesList, mean);

            Stats newStats = new Stats(min,max,mean,std,key[0],key[1],key[2],type);

            return new Tuple2<>(new_key,newStats);
        }

        private double getSTD(List<Double> valuesList, double mean) {
            int sum = 0;

            for (Double i : valuesList)
                sum +=Math.pow((i - mean), 2);
            return Math.sqrt( sum / ( valuesList.size() - 1 ) );
        }

        private double getMean(List<Double> valuesList) {

            double m = 0.0;
            for(Double d : valuesList){
                m += d;
            }
            return m/size(valuesList);
        }
    }

    private static class computeYearlyStats implements PairFunction<Tuple2<String, Iterable<Stats>>, String, YearlyStats> {
        @Override
        public Tuple2<String, YearlyStats> call(Tuple2<String, Iterable<Stats>> stringIterableTuple2) throws Exception {

            String[] key = stringIterableTuple2._1.split("_");
            String new_key = key[0];

            Stats s = getLast(stringIterableTuple2._2);

            YearlyStats yStats = new YearlyStats(key[1],key[0],s.getType());
            for(Stats str : stringIterableTuple2._2){
                yStats.addMonth(str);
            }

            return new Tuple2<>(new_key,yStats);
        }
    }
}
