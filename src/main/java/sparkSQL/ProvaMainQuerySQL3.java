package sparkSQL;

import entities.City;
import entities.MeasureSQLQuery3;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import producer.Producer;
import scala.Tuple2;
import utils_project.Geolocalizer;
import utils_project.TimeDateManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class ProvaMainQuerySQL3 {

    private static String pathToTempFile = "cleaned_dataset/cleaned_temperature.csv";
    private static String pathToCityFile = "dataset/city_attributes.csv";

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Inverted index");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long startTime = System.nanoTime();

        //JavaRDD<String> rawData = sc.textFile(pathToCityFile);
        JavaRDD<String> rawData = sc.textFile("hdfs://localhost:54310/simone/sabd/City.csv");
        HashMap<String, City> city_countries = Geolocalizer.process_city_location(rawData);

        //get temp data
        //JavaRDD<String> tempRawData = sc.textFile(pathToTempFile);
        JavaRDD<String> tempRawData = sc.textFile("hdfs://localhost:54310/simone/sabd/Temperature.csv");

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

                    String key = city+"_"+country+"_"+new_year+"_"+new_month+"_"+new_day+"_"+new_hour;

                    results.add(new Tuple2<>(key,m));
                }

                return results.iterator();
            }
        });

        //filter out data by month and year

        JavaPairRDD<String,MeasureSQLQuery3> dataFilteredByMonth = pairsKeyMeasure.filter(x -> x._1.split("_")[3].equals("01") || x._1.split("_")[3].equals("02") ||
                x._1.split("_")[3].equals("03") || x._1.split("_")[3].equals("04") || x._1.split("_")[3].equals("06") ||
                x._1.split("_")[3].equals("07") || x._1.split("_")[3].equals("08") || x._1.split("_")[3].equals("09"));

        JavaPairRDD<String,MeasureSQLQuery3> dataFilteredByHour = dataFilteredByMonth.filter(x -> x._1.split("_")[5].equals("12") ||x._1.split("_")[5].equals("13") ||
                x._1.split("_")[5].equals("14") || x._1.split("_")[5].equals("15"));

        JavaPairRDD<String,MeasureSQLQuery3> data2k17 = dataFilteredByHour.filter(x -> x._1.split("_")[2].equals("2017"));
        JavaPairRDD<String,MeasureSQLQuery3> data2k16 = dataFilteredByHour.filter(x -> x._1.split("_")[2].equals("2016"));

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> rows2k17 = createSchemaFromData(spark,data2k17);
        rows2k17.createOrReplaceTempView("2k17table");
        Dataset<Row> rows2k16 = createSchemaFromData(spark,data2k16);
        rows2k16.createOrReplaceTempView("2k16table");

        String queryOne = "SELECT country, city, month, AVG(value) AS mean_temp FROM 2k17table GROUP BY country, city, month";
        Dataset<Row> monthMean2017 = spark.sql(queryOne);
        monthMean2017.createOrReplaceTempView("2k17table");

        String queryTwo = "SELECT country, city, mean_temp FROM 2k17table WHERE month=01 OR month=02 OR month=03 OR month=04";
        Dataset<Row> firstQuarter2017 = spark.sql(queryTwo);
        firstQuarter2017.createOrReplaceTempView("2k17FirstTable");

        String queryThree = "SELECT country, city, mean_temp FROM 2k17table WHERE month=06 OR month=07 OR month=08 OR month=09";
        Dataset<Row> secondQuarter2017 = spark.sql(queryThree);
        secondQuarter2017.createOrReplaceTempView("2k17SecondTable");

        String queryFour = "SELECT country, city, AVG(mean_temp) AS first_quarter FROM 2k17FirstTable GROUP BY country,city";
        Dataset<Row> firstQ2017 = spark.sql(queryFour);
        firstQ2017.createOrReplaceTempView("2k17FirstTable");

        String queryFive = "SELECT country, city, AVG(mean_temp) AS second_quarter FROM 2k17SecondTable GROUP BY country,city";
        Dataset<Row> secondQ2017 = spark.sql(queryFive);
        secondQ2017.createOrReplaceTempView("2k17SecondTable");

        String querySix= "SELECT 2k17FirstTable.country, 2k17FirstTable.city, 2k17FirstTable.first_quarter, " +
                "2k17SecondTable.second_quarter FROM 2k17FirstTable LEFT JOIN 2k17SecondTable ON 2k17FirstTable.city=2k17SecondTable.city";
        Dataset<Row> join = spark.sql(querySix);
        join.createOrReplaceTempView("2k17Quarters");

        String querySeven= "SELECT country, city, (first_quarter-second_quarter) AS difference FROM 2k17Quarters";
        Dataset<Row> diff2k17 = spark.sql(querySeven);
        diff2k17.createOrReplaceTempView("2k17Diff");

        String queryEight ="SELECT country, city, ABS(difference) AS diff FROM 2k17Diff";
        Dataset<Row> absDiff2k17 = spark.sql(queryEight);
        absDiff2k17.createOrReplaceTempView("2k17Diff");

        String queryNine = "SELECT city, country, diff FROM 2k17Diff WHERE country='United States' ORDER BY diff DESC";
        Dataset<Row> usa2017 = spark.sql(queryNine);
        usa2017.createOrReplaceTempView("usa2017");

        String queryTen = "SELECT city, country, diff FROM 2k17Diff WHERE country='Israel' ORDER BY diff DESC";
        Dataset<Row> israel2017 = spark.sql(queryTen);
        israel2017.createOrReplaceTempView("israel2017");

        Dataset<Row> usaThree2k17 = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY diff DESC) AS row, city, country, diff FROM usa2017 LIMIT 3");
        usaThree2k17.createOrReplaceTempView("usathree2k17");

        Dataset<Row> israelThree2k17 = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY diff DESC) AS row, city, country, diff FROM israel2017 LIMIT 3");
        israelThree2k17.createOrReplaceTempView("israelthree2k17");

        //<------------- 2016 -------------->

        Dataset<Row> monthMean2016 = spark.sql("SELECT country, city, month, AVG(value) AS mean_temp FROM 2k16table GROUP BY country, city, month");
        monthMean2016.createOrReplaceTempView("2k16table");

        Dataset<Row> firstQuarter2016 = spark.sql( "SELECT country, city, mean_temp FROM 2k16table WHERE month=01 OR month=02 OR month=03 OR month=04");
        firstQuarter2016.createOrReplaceTempView("2k16FirstTable");

        Dataset<Row> secondQuarter2016 = spark.sql( "SELECT country, city, mean_temp FROM 2k16table WHERE month=06 OR month=07 OR month=08 OR month=09");
        secondQuarter2016.createOrReplaceTempView("2k16SecondTable");

        Dataset<Row> firstQ2016 = spark.sql( "SELECT country, city, AVG(mean_temp) AS first_quarter FROM 2k16FirstTable GROUP BY country,city");
        firstQ2016.createOrReplaceTempView("2k16FirstTable");

        Dataset<Row> secondQ2016 = spark.sql("SELECT country, city, AVG(mean_temp) AS second_quarter FROM 2k16SecondTable GROUP BY country,city");
        secondQ2016.createOrReplaceTempView("2k16SecondTable");

        Dataset<Row> join2016 = spark.sql("SELECT 2k16FirstTable.country, 2k16FirstTable.city, 2k16FirstTable.first_quarter, 2k16SecondTable.second_quarter FROM 2k16FirstTable LEFT JOIN 2k16SecondTable ON 2k16FirstTable.city=2k16SecondTable.city"
        );
        join2016.createOrReplaceTempView("2k16Quarters");

        Dataset<Row> diff2k16 = spark.sql("SELECT country, city, (first_quarter-second_quarter) AS difference FROM 2k16Quarters");
        diff2k16.createOrReplaceTempView("2k16Diff");

        Dataset<Row> absDiff2k16 = spark.sql("SELECT country, city, ABS(difference) AS diff FROM 2k16Diff");
        absDiff2k16.createOrReplaceTempView("2k16Diff");

        Dataset<Row> usa2016 = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY diff DESC) AS row, city, country, diff FROM 2k16Diff WHERE country='United States' ORDER BY diff DESC");
        usa2016.createOrReplaceTempView("usa2k16");

        Dataset<Row> israel2016 = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY diff DESC) AS row, city, country, diff FROM 2k16Diff WHERE country='Israel' ORDER BY diff DESC");
        israel2016.createOrReplaceTempView("israel2k16");

    //<---- JOIN 2016 e 2017 ---->

        String queryJoin= "SELECT usathree2k17.country, usathree2k17.row AS row_2017, usathree2k17.city, usa2k16.row AS row_2016 FROM usathree2k17 LEFT JOIN usa2k16 ON usathree2k17.city=usa2k16.city";
        Dataset<Row> usa = spark.sql(queryJoin);

        String queryJoinIsrael= "SELECT israelthree2k17.country, israelthree2k17.row AS row_2017, israelthree2k17.city, israel2k16.row AS row_2016 FROM israelthree2k17 LEFT JOIN israel2k16 ON israelthree2k17.city=israel2k16.city";
        Dataset<Row> israel = spark.sql(queryJoinIsrael);

        Producer producer = new Producer();
        producer.runProducer(usa, "result");
        producer.runProducer(israel, "result");

        long endTime = System.nanoTime();

        System.out.println("\n****\n");

        long timeElapsed = endTime - startTime;

        System.out.println("Time Elapsed (nanoseconds) : " + timeElapsed);
        System.out.println("Time Elapsed (milliseconds) : " + timeElapsed / 1000000);

        sc.stop();
    }

    public static String getMonth(String s){

        String date = s.substring(5,7);
        return date;

    }

    public static String getHour(String s){
        String date = s.substring(11,13);
        return date;
    }

    public static String getYear(String s){
        String date = s.substring(0,4);
        return date;
    }

    private static Dataset<Row> createSchemaFromData(SparkSession spark, JavaPairRDD<String,MeasureSQLQuery3> rdd) {

        List<StructField> fields;
        fields = makeFields();


        JavaRDD<Row> rowrdd = rdd.map(new Function<Tuple2<String, MeasureSQLQuery3>, Row>() {
            @Override
            public Row call(Tuple2<String, MeasureSQLQuery3> stringIterableTuple2) throws Exception {

                String[] key = stringIterableTuple2._1.split("_");


                return RowFactory.create(stringIterableTuple2._2.getCity(),stringIterableTuple2._2.getCountry(),
                        stringIterableTuple2._2.getYear(),stringIterableTuple2._2.getMonth(),
                        stringIterableTuple2._2.getDay(),stringIterableTuple2._2.getHour(), stringIterableTuple2._2.getValue());

            }
        });

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = spark.createDataFrame(rowrdd, schema);

        return df;

    }

    private static List<StructField> makeFields() {

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("city", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("country", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("year", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("month", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("day", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("hour", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.DoubleType, true));

        return fields;
    }
}
