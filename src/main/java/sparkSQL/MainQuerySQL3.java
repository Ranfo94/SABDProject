package sparkSQL;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;
import utils_project.Printer;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.exit;

public class MainQuerySQL3 {

    public static void main(String[] args) {
/*
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query3").master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        //passo lo SparkSessin e l'rdd su cui eseguire la query. In questo caso "counts"
        Dataset<Row> df = createSchemaForQuery3(spark, tempDifference);

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("query3");

        Dataset<Row> result2k17 = spark.sql("SELECT * FROM query3 WHERE year==2017 ORDER BY diffTemp LIMIT 3");
        result2k17.createOrReplaceTempView("city2k17");
        //result2k17 = result2k17.withColumn("id1", functions.monotonically_increasing_id());
        //result2k17.show();

        Dataset<Row> result2k16 = spark.sql("SELECT * FROM query3 WHERE year==2016 ORDER BY diffTemp");
        result2k16.createOrReplaceTempView("temp2k16");
        //result2k16 = result2k16.withColumn("id2", functions.monotonically_increasing_id());
        //result2k16.createOrReplaceTempView("temp2k16");
        //result2k16.show();
        exit(0);
    /*
            Dataset<Row> result = spark.sql("SELECT city FROM query3 WHERE year==2016 INTERSECT (" +
                    " SELECT city FROM query3 WHERE year==2017 ORDER BY diffTemp LIMIT 3)");
    */
    /*
            result.createOrReplaceTempView("temp");
            Dataset<Row> sqlDF = spark.sql("SELECT DISTINCT house_id FROM temp WHERE sum >= 350 ");
    */
    }

    private static Dataset<Row> createSchemaForQuery3(SparkSession spark, JavaPairRDD<String, Double> tempDifference) {

        Printer printer = new Printer();

        JavaRDD<Tuple3<String, String, Double>> tempDiffNew = tempDifference.flatMap(line -> {

            ArrayList<Tuple3<String, String, Double>> result = new ArrayList<>();

            String year = line._1.substring(0,4);
            String city = line._1.substring(5);
            Double value = line._2;

            result.add(new Tuple3<>(year, city, value));
            return result.iterator();
        });
        List<Tuple3<String, String, Double>> list10 = tempDiffNew.collect();
        printer.stampaListaTripla(list10);

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("city",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("diffTemp",     DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = tempDiffNew.map(new Function<Tuple3<String, String, Double>, Row>() {
            @Override
            public Row call(Tuple3<String, String, Double> val) throws Exception {
                return RowFactory.create(val._1(), val._2(), val._3());
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        return df;
    }
}
