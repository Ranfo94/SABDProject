package sparkSQL;

import entities.City;
import entities.MeasureSQL;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;


/**
 * Query 2:
 *
 * Individuare, per ogni nazione, la media, la deviazione standard, il minimo,
 * il massimo della temperatura, della pressione e dell’umidita` registrata in ogni mese di ogni anno.
 */

public class MainQuerySQL2 {

    public static void main (String[] args) throws IOException {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


        long startTime = System.nanoTime();

        HashMap<String, City> city_countries = Query2PreProcessing.getCountriesList(sc);
        List<JavaPairRDD<String, MeasureSQL>> rddList = Query2PreProcessing.preprocessData(sc,city_countries);

        QuerySQL2.process(rddList,city_countries);

        System.out.println("\n****\n");

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;

        System.out.println("Time Elapsed (nanoseconds) : "+timeElapsed);
        System.out.println("Time Elapsed (milliseconds) : "+timeElapsed/1000000);

        spark.close();
        sc.close();
    }
}
