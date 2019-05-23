package utils_project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static utils_project.Parser.findCountryIndex;

public class utilsForQuery3 {

    public static String pathToHumidityFile = "dataset/humidity.csv";
    public static String pathToTemperatureFile = "dataset/temperature.csv";

    //todo acquisire temperature file da hdfs
    public static JavaPairRDD<String, Double> calcolaRdd(ArrayList<String> countries,
                                                         ArrayList<String> distinctCountries) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("query 3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Parser parser = new Parser();
        Printer printer = new Printer();

        ArrayList<String> cities = parser.findCities(pathToHumidityFile);

        ArrayList<String> temperatureFile = parser.filtraFile(pathToTemperatureFile);
        JavaRDD<String> temperature = sc.parallelize(temperatureFile);

        sc.close();

        //ricavo gli rdd relativi al 2017
        JavaRDD<String> temperature2k = temperature.filter(line -> (line.contains("2016-") || line.contains("2017-"))
                && (line.contains("12:00") || line.contains("13:00") || line.contains("14:00") || line.contains("15:00")));

        //creo coppie chiave-valore con chiave = "datetime città periodo" e valore = temperatura di una certa ora
        JavaPairRDD<String, Double> coppia2 = temperature2k.flatMapToPair(line -> {
            ArrayList<Tuple2<String, Double>> result = new ArrayList<>();
            String key;
            String period;
            Double dailyTemperature;

            String[] word = line.split(",");
            String date = word[0].substring(0, 10);

            if (date.contains("-06-") || date.contains("-07-") || date.contains("-08-") || date.contains("-09-"))
                period = "period_1";
            else
                period = "period_2";

            int i;
            for (i = 1; i < word.length; ++i) {
                String country = countries.get(i-1);
                Integer countryIndex = findCountryIndex(country, distinctCountries);
                key = date + " " + period + " " + countryIndex.toString() + " " + cities.get(i - 1);
                dailyTemperature = Double.valueOf(word[i]);
                result.add(new Tuple2<>(key, dailyTemperature));
            }
            return result.iterator();
        });

        List<Tuple2<String, Double>> listaGiornaliera = coppia2.collect();
        printer.stampaListaGiornaliera(listaGiornaliera);
        //exit(0);

        //calcolo la temperatura nella fascia oraria
        JavaPairRDD<String, Double> dailyTemp = coppia2.reduceByKey((x, y) -> x + y);

        JavaPairRDD<String, Double> dailyAvgTemp = dailyTemp.flatMapToPair(line -> {

            ArrayList<Tuple2<String, Double>> result = new ArrayList<>();

            //è la temperatura media giornaliera nella fascia oraria
            Double dailyAvgTempValue = line._2 / 4.0;

            //la chiave non ha il giorno. L'obiettivo è calcolare la temperatura media mensile
            String key = line._1.substring(0, 7) + line._1.substring(10);

            result.add(new Tuple2<>(key, dailyAvgTempValue));
            return result.iterator();
        });
        /*
        List<Tuple2<String, Double>> lista = dailyAvgTemp.collect();
        printer.stampaListaMensile(lista);
        //exit(0);
         */

        JavaPairRDD<String, Double> montlyTemp = dailyAvgTemp.reduceByKey((x, y) -> x+y);

        JavaPairRDD<String, Double> montlyAvgTemp = montlyTemp.flatMapToPair(line -> {

            ArrayList<Tuple2<String, Double>> result = new ArrayList<>();

            Integer montlyDay;
            if(line._1.contains("-02"))
                montlyDay = 28;
            else if(line._1.contains("-04") || line._1.contains("-06") || line._1.contains("-09") || line._1.contains("-11"))
                montlyDay = 30;
            else
                montlyDay = 31;
            Double montlyAvgTempValue = line._2 / montlyDay;

            String key = line._1.substring(0, 4) + line._1.substring(7);

            result.add(new Tuple2<>(key, montlyAvgTempValue));
            return result.iterator();
        });
        /*
        List<Tuple2<String, Double>> lista2 = montlyAvgTemp.collect();
        printer.stampaListaAnnuale(lista2);
        //exit(0);
         */

        JavaPairRDD<String, Double> yearlyTemp = montlyAvgTemp.reduceByKey((x, y) -> x+y);

        JavaPairRDD<String, Double> yearlyAvgTemp = yearlyTemp.flatMapToPair(line -> {
            ArrayList<Tuple2<String, Double>> result = new ArrayList<>();

            //String key = line._1.substring(0, line._1.length()-9);
            String key = line._1.substring(0,4) + " " + line._1.substring(14);
            /*calcolo il valore annuale medio della temperatura. L'idea è di avere valori positivi relativi al 2017 e
            negativi per il 2016
             */

            Double yearlyAvgTempValue = line._2 / 12.0;
            if(line._1.contains("period_1") && line._2.doubleValue()<0)
                yearlyAvgTempValue = -yearlyAvgTempValue;
            if(line._1.contains("period_2") && line._2.doubleValue()>0)
                yearlyAvgTempValue = -yearlyAvgTempValue;

            result.add(new Tuple2<>(key, yearlyAvgTempValue));
            return result.iterator();
        });
        /*
        List<Tuple2<String, Double>> listaAnno = yearlyAvgTemp.collect();
        printer.stampaListaAnnuale(listaAnno);
        //exit(0);
         */

        JavaPairRDD<String, Double> tempDifference = yearlyAvgTemp.reduceByKey((x, y) -> x+y);
        return tempDifference;
    }
}
