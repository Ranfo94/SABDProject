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
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class QuerySQL1 {

    public static void process (JavaPairRDD<String,WeatherDescriptionSQL> values) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


        Dataset<Row> rows = createSchemaFromData(spark,values);

        rows.createOrReplaceTempView("descr");

        //sum of sunny hours per day
        //result: year | city | month | day | count of sunny hours
        String queryCount = "SELECT year, city, month, day, SUM(sunny) AS sunny FROM descr GROUP BY year,city,month,day";
        Dataset<Row> sunnyHoursPerDayCount = spark.sql(queryCount);
        sunnyHoursPerDayCount.createOrReplaceTempView("descr");

        // select where sunny hours per day > 12
        //result: year | city | month | day
        String queryMonth = "SELECT year, city, month, day FROM descr WHERE sunny > 14 ";
        Dataset<Row> sunnyDaysPerMonth = spark.sql(queryMonth);
        sunnyDaysPerMonth.createOrReplaceTempView("descr");

        //count of sunny days per month
        //result: year | city | month | count of sunny days
        String queryCountDays = "SELECT year, city, month, count(*) AS count FROM descr GROUP BY year,city, month";
        Dataset<Row> sunnyDaysCount = spark.sql(queryCountDays);
        sunnyDaysCount.createOrReplaceTempView("descr");

        //select where count of sunny days per month > 14
        //result: year | city | month
        //months with more than 14 sunny days
        String querySunnyMonths = "SELECT year, city, month FROM descr WHERE count > 14";
        Dataset<Row> sunnyMonths = spark.sql(querySunnyMonths);
        sunnyMonths.createOrReplaceTempView("descr");

        //count number of months with at least 15 sunny days
        //result: year | city | month count
        String queryCountSunnyMonths = "SELECT year, city, count(*) AS count FROM descr GROUP BY year, city ";
        Dataset<Row> sunnyMonthsCount = spark.sql(queryCountSunnyMonths);
        sunnyMonthsCount.createOrReplaceTempView("descr");

        //select where count of months with at least 15 sunny days = 3
        //result: year | city
        String queryCitiesYears = "SELECT year, city FROM descr WHERE count =3";
        Dataset<Row> citiesYears = spark.sql(queryCitiesYears);
        citiesYears.createOrReplaceTempView("descr");

        //result ordered by year
        String orderedCitiesYear = "SELECT * FROM descr ORDER BY year ASC";
        Dataset<Row> orderedCitiesYears = spark.sql(orderedCitiesYear);
        orderedCitiesYears.createOrReplaceTempView("descr");

        orderedCitiesYears.show();

    }

    private static Dataset<Row> createSchemaFromData(SparkSession spark, JavaPairRDD<String,WeatherDescriptionSQL> values) {

        List<StructField> fields;
        fields = makeFields();

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rddRow = values.map(new makeRow());

        Dataset<Row> rows = spark.createDataFrame(rddRow,schema);

        return rows;

    }

    private static class makeRow implements Function<Tuple2<String, WeatherDescriptionSQL>, Row> {

        @Override
        public Row call(Tuple2<String, WeatherDescriptionSQL> stringWeatherDescriptionSQLTuple2) throws Exception {

            //key: (city_day_month_year)
            String[] key = stringWeatherDescriptionSQLTuple2._1.split("_");
            WeatherDescriptionSQL wd = stringWeatherDescriptionSQLTuple2._2;
            String city = wd.getCity();
            String month = wd.getMonth();
            String day = wd.getDay();
            String year = wd.getYear();
            String w_descr = wd.getDescription();

            int sunny=0;

            if(w_descr.contains("sky is clear")){
                sunny +=1;
            }

            return RowFactory.create(year, city, month, day, sunny);

        }
    }

    private static List<StructField> makeFields() {

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("city",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("month",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("day",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sunny",      DataTypes.IntegerType, true));

        return fields;

    }
}
