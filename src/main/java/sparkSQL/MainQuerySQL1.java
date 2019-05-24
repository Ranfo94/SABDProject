package sparkSQL;

import entities.WeatherDescriptionSQL;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * Query: Per ogni anno del dataset
 * individuare le citta` che hanno almeno 15 giorni al mese di tempo sereno nei mesi di marzo, aprile e maggio.
 *
 * file: weather_description.csv
 *
 * output: per ogni anno, elenco delle città
 */

public class MainQuerySQL1 {


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

        JavaPairRDD<String, WeatherDescriptionSQL> processedData =Query1PreProcessing.preprocessData(sc);
        QuerySQL1.process(processedData);

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;

        System.out.println("Time Elapsed (nanoseconds) : "+timeElapsed);
        System.out.println("Time Elapsed (milliseconds) : "+timeElapsed/1000000);

        spark.close();
        sc.close();
    }
}
