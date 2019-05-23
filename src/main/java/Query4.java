import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import sparkSQL.MainQuerySQL3;
import utils_project.Geolocalizer;
import utils_project.Parser;
import utils_project.Printer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.exit;
import static utils_project.Parser.findCountryIndex;
import static utils_project.Parser.findDistinctCountries;
import static utils_project.utilsForQuery3.*;

public class Query4 {

    private static String pathToHumidityFile = "cleaned_dataset/cleaned_humidity.csv";
    private static String pathToTemperatureFile = "cleaned_dataset/cleaned_temperature.csv";

    public static String pathToCityFile = "dataset/city_attributes.csv";

    public static void main(String[] args) throws IOException, URISyntaxException {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("query 3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //******************TEST****************************

        readAvro();
        exit(0);

        //*************************************************

        Parser parser = new Parser();
        Printer printer = new Printer();
        Geolocalizer geolocalizer = new Geolocalizer();

        ArrayList<String> countries = geolocalizer.findCountries(pathToCityFile);

        ArrayList<String> distinctCountries = findDistinctCountries(countries);
        System.out.println(distinctCountries);

        ArrayList<String> cities = parser.findCities(pathToHumidityFile);

        ArrayList<String> temperatureFile = parser.filtraFile(pathToTemperatureFile);
        JavaRDD<String> temperature = sc.parallelize(temperatureFile);

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

        JavaPairRDD<Double, String> reverseRdd = tempDifference.flatMapToPair(line -> {
            ArrayList<Tuple2<Double, String>> result = new ArrayList<>();

            String key = line._1;
            Double value = line._2;
            if(value<0)
                value = -value;

            result.add(new Tuple2<>(value, key));
            return result.iterator();
        }).sortByKey(false);
        /*
        List<Tuple2<Double, String>> list9 = reverseRdd.collect();
        printer.stampaRisultato(list9);
        //exit(0);
         */

        int i;
        for(i=0; i<distinctCountries.size(); ++i)
            findRanking(reverseRdd, i, distinctCountries);
 /*
        //salvataggio in HDFS
        //caso 1: locale    caso 2: container
        //classifica2k17.saveAsTextFile("hdfs://localhost:9000/sabd/output");
        //classifica2k17.saveAsTextFile("hdfs://localhost:54310/simone/sabd/output_query3");

        //result.show();
  */
        /*
        Writer writer = new Writer();
        writer.writeString(distinctCountries);
         */
        //writer.writeRdd(reverseRdd);

        new MainQuerySQL3().eseguiQuerySQL3(tempDifference, sc, distinctCountries);
        sc.close();
    }

    private static void readAvro() {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query3").master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        //com.databricks.spark.avro
        Dataset<Row> df = spark.read().format("avro")
                .load("hdfs://localhost:54310/simone/sabd/temperature.avro");
        df.createOrReplaceTempView("words");
        Dataset<Row> wordCountsDataFrame = spark.sql("select count(*) as total from words");
        wordCountsDataFrame.show();
        df.show();
    }


    /**
     * Dato un rdd che ha per chiave la differenza di temperatura tra due quadrimestri del 2017 e 2016, filtra per
     * ottenere un rdd per ogni anno.
     * Infine viene chiamata la funzione "stampaClassifiche()" per stampare i primi tre paesi nella classifica del
     * 2017 e le rispettive posizioni in quella del 2016.
     * @param reverseRdd
     */
    public static void findRanking(JavaPairRDD<Double, String> reverseRdd, Integer i, ArrayList<String> countries){

        Printer printer = new Printer();

        reverseRdd = reverseRdd.filter(line -> line._2.substring(5).contains(i.toString()));

        JavaPairRDD<Double, String> classifica2k17 = reverseRdd.filter(pair -> pair._2.contains("2017"));
        List<Tuple2<Double, String>> listTop2k17 = classifica2k17.collect();
        printer.stampaRisultato(listTop2k17);

        JavaPairRDD<Double, String> classifica2k16 = reverseRdd.filter(pair -> pair._2.contains("2016"));
        List<Tuple2<Double, String>> listTop2k16 = classifica2k16.collect();
        printer.stampaRisultato(listTop2k16);

        stampaClassifiche(classifica2k17, classifica2k16, countries, i);
    }


    /**
     * La funzione stampa i primi tre paesi nella classifica del 2017, quindi li cerca nella classifica del 2016
     * per stamparne le rispettive posizioni.
     * @param classifica2k17
     * @param classifica2k16
     */
    public static void stampaClassifiche(JavaPairRDD<Double, String> classifica2k17, JavaPairRDD<Double,
            String> classifica2k16, ArrayList<String> countries, Integer index){

        List<Tuple2<Double, String>> top2k17 = classifica2k17.take(3);
        System.out.println("country:" + countries.get(index.intValue()));

        System.out.println("classifica 2017:");
        System.out.println("1^ posizione: " + top2k17.get(0)._2.substring(4));
        System.out.println("2^ posizione: " + top2k17.get(1)._2.substring(4));
        System.out.println("3^ posizione: " + top2k17.get(2)._2.substring(4));

        List<Tuple2<Double, String>> top2k16 = classifica2k16.collect();
        int i, j, k;
        boolean trovato;
        System.out.println("country:" + countries.get(index.intValue()));

        System.out.println("rispettive posizioni nella classifica 2016:");

        for(i=0; i<top2k17.size(); ++i){
            trovato = false;
            for(j=0; j<top2k16.size() && !trovato; ++j){
                if(top2k16.get(j)._2.contains(top2k17.get(i)._2.substring(4))) {
                    k = j + 1;
                    System.out.println(k + "^ posizione: " + top2k16.get(j)._2.substring(4));
                    trovato = true;
                }
            }
        }
    }

    public static void query3new() throws IOException {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("query 3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Parser parser = new Parser();
        Printer printer = new Printer();
        Geolocalizer geolocalizer = new Geolocalizer();

        ArrayList<String> countries = geolocalizer.findCountries(pathToCityFile);

        ArrayList<String> distinctCountries = findDistinctCountries(countries);
        System.out.println(distinctCountries);

        ArrayList<String> cities = parser.findCities(pathToHumidityFile);

        ArrayList<String> temperatureFile = parser.filtraFile(pathToTemperatureFile);
        JavaRDD<String> temperature = sc.parallelize(temperatureFile);

        //ricavo gli rdd relativi alla fascia oraria 12.00-15.00 per il 2016 e il 2017
        JavaRDD<String> temperature2k = temperature.filter(line -> (line.contains("2016-") || line.contains("2017-"))
                && (line.contains("12:00") || line.contains("13:00") || line.contains("14:00") || line.contains("15:00")));

        //creo coppie chiave-valore con chiave = "datetime città periodo" e valore = temperatura di una certa ora
        JavaPairRDD<String, Double> dailyTemp = temperature2k.flatMapToPair(line -> {
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
        }).reduceByKey((x, y) -> x + y);


        JavaPairRDD<String, Double> montlyTemp = dailyTemp.flatMapToPair(line -> {

            ArrayList<Tuple2<String, Double>> result = new ArrayList<>();

            //è la temperatura media giornaliera nella fascia oraria
            Double dailyAvgTempValue = line._2 / 4.0;

            //la chiave non ha il giorno. L'obiettivo è calcolare la temperatura media mensile
            String key = line._1.substring(0, 7) + line._1.substring(10);

            result.add(new Tuple2<>(key, dailyAvgTempValue));
            return result.iterator();
        }).reduceByKey((x, y) -> x+y);

        JavaPairRDD<String, Double> yearlyTemp = montlyTemp.flatMapToPair(line -> {

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
        }).reduceByKey((x, y) -> x+y);


        JavaPairRDD<String, Double> tempDifference = yearlyTemp.flatMapToPair(line -> {
            ArrayList<Tuple2<String, Double>> result = new ArrayList<>();

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
        }).reduceByKey((x, y) -> x+y);


        JavaPairRDD<Double, String> reverseRdd = tempDifference.flatMapToPair(line -> {
            ArrayList<Tuple2<Double, String>> result = new ArrayList<>();

            String key = line._1;
            Double value = line._2;
            if(value<0)
                value = -value;

            result.add(new Tuple2<>(value, key));
            return result.iterator();
        }).sortByKey(false);

        int i;
        for(i=0; i<distinctCountries.size(); ++i)
            findRanking(reverseRdd, i, distinctCountries);
 /*
        //salvataggio in HDFS
        //caso 1: locale    caso 2: container
        //classifica2k17.saveAsTextFile("hdfs://localhost:9000/sabd/output");
        //classifica2k17.saveAsTextFile("hdfs://localhost:54310/simone/sabd/output_query3");

        //result.show();
  */
        /*
        Writer writer = new Writer();
        writer.writeString(distinctCountries);
         */
        //writer.writeRdd(reverseRdd);

        new MainQuerySQL3().eseguiQuerySQL3(tempDifference, sc, distinctCountries);
        sc.close();
    }
}
