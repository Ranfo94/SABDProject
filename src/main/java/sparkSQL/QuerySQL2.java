package sparkSQL;

import entities.City;
import entities.MeasureSQL;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QuerySQL2 {

    public static void process(List<JavaPairRDD<String, MeasureSQL>> list, HashMap<String, City> city_countries){

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        List<String> countries = new ArrayList<>();
        for(String k : city_countries.keySet()){
            String c = city_countries.get(k).getCountry();
            if(countries.contains(c)==false){
                countries.add(c);
            }
        }

        /**
         * TEMPERATURE
         */

        Dataset<Row> dsTemp = createSchemaFromData(spark,list.get(0),city_countries);
        String nameTemp = "temperature";
        dsTemp.createOrReplaceTempView(nameTemp);
        String queryTempStats = "SELECT year, country, month, AVG(value), MAX(value), MIN(value), STD(value) FROM "+nameTemp+" GROUP BY month,country,year";
        Dataset<Row> tempRes = spark.sql(queryTempStats);
        tempRes.createOrReplaceTempView(nameTemp);
        String queryTempSort = "SELECT * FROM "+nameTemp+" ORDER BY year, country, month ASC";
        Dataset<Row> sortedTempRes = spark.sql(queryTempSort);
        sortedTempRes.createOrReplaceTempView(nameTemp);

        System.out.println("TEMPERATURE\n");
        sortedTempRes.show();

        //TODO: INVIA RISULTATO SU HDFS. FILE: sortedTempRes


        /**
         * HUMIDITY
         */

        Dataset<Row> dsHum = createSchemaFromData(spark,list.get(1),city_countries);
        String nameHum = "humidity";
        dsHum.createOrReplaceTempView(nameHum);
        String queryHumStats = "SELECT year, country, month, AVG(value), MAX(value), MIN(value), STD(value) FROM "+nameHum+" GROUP BY month,country,year";
        Dataset<Row> humRes = spark.sql(queryHumStats);
        humRes.createOrReplaceTempView(nameHum);
        String queryHumSort = "SELECT * FROM "+nameHum+" ORDER BY year, country, month ASC";
        Dataset<Row> sortedHumRes = spark.sql(queryHumSort);
        sortedHumRes.createOrReplaceTempView(nameHum);

        System.out.println("HUMIDITY\n");
        sortedHumRes.show();

        //TODO: INVIA RISULTATO SU HDFS. FILE: sortedHumRes

        /**
         * PRESSURE
         */

        Dataset<Row> dsPress = createSchemaFromData(spark,list.get(2),city_countries);
        String namePress = "pressure";
        dsPress.createOrReplaceTempView(namePress);
        String queryPressStats = "SELECT year, country, month, AVG(value), MAX(value), MIN(value), STD(value) FROM "+namePress+" GROUP BY month,country,year";
        Dataset<Row> pressRes = spark.sql(queryPressStats);
        pressRes.createOrReplaceTempView(namePress);
        String queryPressSort = "SELECT * FROM "+namePress+" ORDER BY year, country, month ASC";
        Dataset<Row> sortedPressRes = spark.sql(queryPressSort);
        sortedPressRes.createOrReplaceTempView(namePress);

        System.out.println("PRESSURE\n");
        sortedPressRes.show();

        //TODO: INVIA RISULTATO SU HDFS. FILE: SORTEDPRESSRESS

    }

    /**
     * make table
     * input:(year_month_date_hour, [measures, ...]);
     * colonne: giorni + mese
     */
    private static Dataset<Row> createSchemaFromData(SparkSession spark,JavaPairRDD<String,MeasureSQL> rdd,HashMap<String,City> countries) {

        //JavaPairRDD<String,MeasureSQL> rdd = list.get(0).filter(x -> x._1.split("_")[0].equals(year));

        List<StructField> fields;
        fields = makeFields();


        JavaRDD<Row> rowrdd = rdd.map(new Function<Tuple2<String, MeasureSQL>, Row>() {
            @Override
            public Row call(Tuple2<String, MeasureSQL> stringIterableTuple2) throws Exception {

                String[] key = stringIterableTuple2._1.split("_");
                String year = key[0];
                String month = key[1];
                String country = stringIterableTuple2._2.getCountry();
                Double value = stringIterableTuple2._2.getValue();

                return RowFactory.create(country,year,month,value);

                }
        });

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = spark.createDataFrame(rowrdd, schema);


        return df;

    }

    private static List<String> getYearList(JavaPairRDD<String,MeasureSQL> valuesTemperature) {

        Map<String,MeasureSQL> map = valuesTemperature.collectAsMap();
        List<String> list = new ArrayList<>();
        for(String k : map.keySet()){
            String y = k.split("_")[0];
            if(list.contains(y)==false){
                list.add(y);
            }
        }
        return list;
    }

    private static List<StructField> makeFields() {

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("country", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("year", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("month", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.DoubleType, true));


        return fields;
    }


}
