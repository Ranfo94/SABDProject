import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import utils_project.Geolocalizer;
import utils_project.Parser;
import utils_project.Printer;
import utils_project.Writer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.exit;


public class Query3 {

    public static String pathToHumidityFile = "dataset/humidity.csv";
    public static String pathToTemperatureFile = "dataset/temperature.csv";
    private static String pathToCityFile = "dataset/city_attributes.csv";


    public static void main(String[] args) throws IOException, URISyntaxException {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("query 3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Parser parser = new Parser();
        Printer printer = new Printer();
        Geolocalizer geolocalizer = new Geolocalizer();

        ArrayList<String> cities = parser.findCities(pathToHumidityFile);
        ArrayList<String> countries = geolocalizer.findCountries(pathToCityFile);

        /*****************
        TEST
        *****************/
/*
        JavaRDD<String> cityFile = sc.textFile(pathToCityFile);

        //elimino la prima riga del file
        String firstRow = cityFile.first();
        JavaRDD<String> cityRawData = cityFile.filter(x -> !x.equals(firstRow));

        JavaRDD<String> cityData = cityRawData.flatMap(line -> {

            ArrayList<String> result = new ArrayList<>();

            //word contiene nome citta, lat e long
            String[] word = line.split(",");

            String country = new Geolocalizer().localize(word[1], word[2]);
            String key = country;
            /*
            long lat = (long) Double.parseDouble(word[1]);
            long lon = (long) Double.parseDouble(word[2]);

            String country = new Geolocalizer().localize(word[1],word[2]);
            int offset = TimeDateManager.getTimeZoneOffset(lat,lon);
            City city = new City(word[0],country,lat,lon,offset);
            */
/*
            result.add(key);
            return result.iterator();
        });
        /*
        List<String> listCity = cityData.collect();
        printer.stampaCollezione(listCity);
        //exit(0);
*/
        /****************
         *
         */

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
                //key = date + " " + cities.get(i - 1) + " " + period;
                key = date + " " + period + " " + cities.get(i - 1) + " " + countries.get(i-1);
                dailyTemperature = Double.valueOf(word[i]);
                result.add(new Tuple2<>(key, dailyTemperature));
            }
            return result.iterator();
        });
/*
        List<Tuple2<String, Double>> listaGiornaliera = coppia2.collect();
        printer.stampaListaGiornaliera(listaGiornaliera);
        //exit(0);
 */

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


        //todo funzione che dinamicamente gestisce usa e israel
        ArrayList<String> distinctCountries = findDistinctCountries(countries);
        System.out.println(distinctCountries);

        int i;
        for(i=0; i<distinctCountries.size(); ++i)
            findRanking(reverseRdd, distinctCountries.get(i));
 /*
        //caso 1: locale    caso 2: container
        //classifica2k17.saveAsTextFile("hdfs://localhost:9000/sabd/output");
        //classifica2k17.saveAsTextFile("hdfs://localhost:54310/simone/sabd/output_query3");

        //result.show();
  */
        Writer writer = new Writer();
        writer.writeString(distinctCountries);
        //writer.writeRdd(reverseRdd);
        sc.close();
    }

    /**
     * Cerca le singole nazioni presenti nella lista di città
     * @param countries
     * @return
     */
    static public ArrayList<String> findDistinctCountries(ArrayList<String> countries){
        ArrayList<String> result = new ArrayList<>();

        int i;
        for(i=0; i<countries.size(); ++i){
            if(!result.contains(countries.get(i)))
                result.add(countries.get(i));
        }
        return result;
    }

    /**
     * Dato un rdd che ha per chiave la differenza di temperatura tra due quadrimestri del 2017 e 2016, filtra per
     * ottenere un rdd per ogni anno.
     * Infine viene chiamata la funzione "stampaClassifiche()" per stampare i primi tre paesi nella classifica del
     * 2017 e le rispettive posizioni in quella del 2016.
     * @param reverseRdd
     * @param city
     */
    static public void findRanking(JavaPairRDD<Double, String> reverseRdd, String city){

        Printer printer = new Printer();

        reverseRdd = reverseRdd.filter(line -> line._2.contains(city));

        JavaPairRDD<Double, String> classifica2k17 = reverseRdd.filter(pair -> pair._2.contains("2017"));
        List<Tuple2<Double, String>> listTop2k17 = classifica2k17.collect();
        printer.stampaRisultato(listTop2k17);

        JavaPairRDD<Double, String> classifica2k16 = reverseRdd.filter(pair -> pair._2.contains("2016"));
        List<Tuple2<Double, String>> listTop2k16 = classifica2k16.collect();
        printer.stampaRisultato(listTop2k16);

        stampaClassifiche(classifica2k17, classifica2k16);
    }

    /**
     * La funzione stampa i primi tre paesi nella classifica del 2017, quindi li cerca nella classifica del 2016
     * per stamparne le rispettive posizioni.
     * @param classifica2k17
     * @param classifica2k16
     */
    static public void stampaClassifiche(JavaPairRDD<Double, String> classifica2k17, JavaPairRDD<Double, String> classifica2k16){

        List<Tuple2<Double, String>> top2k17 = classifica2k17.take(3);
        System.out.println("classifica 2017:");
        System.out.println("1^ posizione: " + top2k17.get(0)._2.substring(4));
        System.out.println("2^ posizione: " + top2k17.get(1)._2.substring(4));
        System.out.println("3^ posizione: " + top2k17.get(2)._2.substring(4));

        List<Tuple2<Double, String>> top2k16 = classifica2k16.collect();
        int i, j, k;
        boolean trovato;
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
}
