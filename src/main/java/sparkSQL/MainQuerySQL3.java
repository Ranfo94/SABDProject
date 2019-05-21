package sparkSQL;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple4;
import utils_project.Printer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public class MainQuerySQL3 {

    public void eseguiQuerySQL3(JavaPairRDD<String, Double> tempDifference, JavaSparkContext sc,
                                ArrayList<String> countries) throws IOException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query3").master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        //passo lo SparkSession e l'rdd su cui eseguire la query. In questo caso "tempDifference"
        Dataset<Row> df = createSchemaForQuery3(spark, tempDifference, countries);

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("query3");

        //todo rendere dinamico. Problema con "", accetta solo ''
        //CASO 1 UNITED STATES
        Dataset<Row> result2k17 = spark.sql("SELECT * FROM query3 WHERE year==2017 AND country='United States' " +
                "ORDER BY diffTemp LIMIT 3");

        //aggiungo colonna "id" basata sul valore della temperatura
        WindowSpec window = Window.orderBy("diffTemp");
        result2k17 = result2k17.withColumn("rank2k17", row_number().over(window));

        result2k17.createOrReplaceTempView("result2k17");
        //result2k17.show();

        Dataset<Row> result2k16 = spark.sql("SELECT * FROM query3 WHERE year==2016 AND country=='United States' " +
                "ORDER BY diffTemp");
        result2k16 = result2k16.withColumn("rank2k16", row_number().over(window));
        result2k16.createOrReplaceTempView("result2k16");
        //result2k16.show();
        //exit(0);

        Dataset<Row> result = spark.sql("SELECT r17.city, r17.rank2k17, r16.city, r16.rank2k16 " +
                "FROM result2k16 r16 JOIN result2k17 r17 ON r17.city==r16.city");
        result.show();

        //CASO 2 ISRAEL
        result2k17 = spark.sql("SELECT * FROM query3 WHERE year==2017 AND country='Israel' " +
                "ORDER BY diffTemp LIMIT 3");

        //aggiungo colonna "id" basata sul valore della temperatura
        window = Window.orderBy("diffTemp");
        result2k17 = result2k17.withColumn("rank2k17", row_number().over(window));

        result2k17.createOrReplaceTempView("result2k17");
        //result2k17.show();

        result2k16 = spark.sql("SELECT * FROM query3 WHERE year==2016 AND country=='Israel' " +
                "ORDER BY diffTemp");
        result2k16 = result2k16.withColumn("rank2k16", row_number().over(window));
        result2k16.createOrReplaceTempView("result2k16");
        //result2k16.show();
        //exit(0);

        result = spark.sql("SELECT r17.city, r17.rank2k17, r16.city, r16.rank2k16 " +
                "FROM result2k16 r16 JOIN result2k17 r17 ON r17.city==r16.city");
        result.show();
    }


    private static Dataset<Row> createSchemaForQuery3(SparkSession spark, JavaPairRDD<String,
            Double> tempDifference, ArrayList<String> countries) {

        List<Tuple2<String, Double>> lista = tempDifference.collect();
        System.out.println(lista.get(0)._1);
        //exit(0);

        Printer printer = new Printer();

        JavaRDD<Tuple4<String, String, String, Double>> tempDiffNew = tempDifference.flatMap(line -> {

            ArrayList<Tuple4<String, String, String, Double>> result = new ArrayList<>();

            String year = line._1.substring(0,4);

            String stringIndex = line._1.substring(5,6);
            int index = Integer.parseInt(stringIndex);
            String country = countries.get(index);

            String city=line._1.substring(7);
            Double value = line._2;

            result.add(new Tuple4<>(year, city, country, value));
            return result.iterator();
        });
        List<Tuple4<String, String, String, Double>> list10 = tempDiffNew.collect();
        printer.stampaListaQuadrupla(list10);
        //exit(0);

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("city",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("country",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("diffTemp",     DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = tempDiffNew.map(new Function<Tuple4<String, String, String, Double>, Row>() {
            @Override
            public Row call(Tuple4<String, String, String, Double> val) throws Exception {
                return RowFactory.create(val._1(), val._2(), val._3(), val._4());
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        return df;

/*
        JavaRDD<Tuple4<String, String, String, Double>> tempDiffNew = tempDifference.flatMap(line -> {

            ArrayList<Tuple4<String, String, String, Double>> result = new ArrayList<>();

            String year = line._1.substring(0,4);
            String city = line._1.substring(5);
            String country = "";
            Double value = line._2;

            result.add(new Tuple4<>(year, city, country, value));
            return result.iterator();
        });
        List<Tuple4<String, String, String, Double>> list10 = tempDiffNew.collect();
        printer.stampaListaQuadrupla(list10);
        exit(0);
        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("city",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("country",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("diffTemp",     DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = tempDiffNew.map(new Function<Tuple4<String, String, String, Double>, Row>() {
            @Override
            public Row call(Tuple4<String, String, String, Double> val) throws Exception {
                return RowFactory.create(val._1(), val._2(), val._3(), val._4());
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        return df;
*/
    }
}
