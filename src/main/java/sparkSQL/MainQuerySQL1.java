package sparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.List;

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


        JavaPairRDD<String,Iterable<Integer>> processedData =Query1PreProcessing.preprocessData(sc);

        List<String> yearList = Query1PreProcessing.getYearList(processedData);

        Query1SQL.process(processedData,yearList);
        spark.close();
        sc.close();
    }
}
