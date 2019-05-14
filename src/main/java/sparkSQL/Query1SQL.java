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


public class Query1SQL {

    public static void process (JavaPairRDD<String,Iterable<Integer>> values, List<String> years) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        for(String s : years) {

            String year = s;

            List<Dataset<Row>> listDf = createSchemaFromData(spark, values, year);

            Dataset<Row> dfMarch = listDf.get(0);
            Dataset<Row> dfApril = listDf.get(1);
            Dataset<Row> dfMay = listDf.get(2);

            dfMarch.createOrReplaceTempView("march");
            dfApril.createOrReplaceTempView("april");
            dfMay.createOrReplaceTempView("may");

            String queryMarch = "SELECT city, (one+two+three+four+five+six+seven+eight+" +
                    "nine+ten+eleven+twelve+thirteen+fourteen+fifteen+sixteen+seventeen+" +
                    "eighteen+nineteen+twenty+twentyone+twentytwo+twentythree+twentyfour+twentyfive+" +
                    "twentysix+twentyseven+twentyeight+twentynine+thirty+thirtyone) AS tot_march FROM march";

            Dataset<Row> resultMarch = spark.sql(queryMarch);

            resultMarch.createOrReplaceTempView("march");

            String queryApril = "SELECT city, (one+two+three+four+five+six+seven+eight+" +
                    "nine+ten+eleven+twelve+thirteen+fourteen+fifteen+sixteen+seventeen+" +
                    "eighteen+nineteen+twenty+twentyone+twentytwo+twentythree+twentyfour+twentyfive+" +
                    "twentysix+twentyseven+twentyeight+twentynine+thirty) AS tot_april FROM april";

            Dataset<Row> resultApril = spark.sql(queryApril);

            resultApril.createOrReplaceTempView("april");

            String queryMay = "SELECT city, (one+two+three+four+five+six+seven+eight+" +
                    "nine+ten+eleven+twelve+thirteen+fourteen+fifteen+sixteen+seventeen+" +
                    "eighteen+nineteen+twenty+twentyone+twentytwo+twentythree+twentyfour+twentyfive+" +
                    "twentysix+twentyseven+twentyeight+twentynine+thirty+thirtyone) AS tot_may FROM may";

            Dataset<Row> resultMay = spark.sql(queryMay);

            resultMay.createOrReplaceTempView("may");

            String firstJoinQuery = "SELECT march.city, march.tot_march, april.tot_april FROM march LEFT JOIN april ON march.city=april.city";
            Dataset<Row> firstJoin = spark.sql(firstJoinQuery);

            firstJoin.createOrReplaceTempView("marchapril");

            String secondJoinQuery = "SELECT marchapril.city, marchapril.tot_march, marchapril.tot_april, may.tot_may FROM marchapril LEFT JOIN may ON marchapril.city=may.city";
            Dataset<Row> secondJoin = spark.sql(secondJoinQuery);

            secondJoin.createOrReplaceTempView("marchaprilmay");

            //secondJoin.show();

            String getResult = "SELECT city FROM marchaprilmay WHERE (tot_march > 14 AND tot_april > 14 AND tot_may >14)";
            Dataset<Row> result = spark.sql(getResult);

            System.out.println("Year "+year);
            result.show();
        }

        spark.close();

    }

    private static List<Dataset<Row>> createSchemaFromData(SparkSession spark, JavaPairRDD<String,Iterable<Integer>> values,String year) {

        List<StructField> fields31;
        fields31 = makeFields("31");

        List<StructField> fields30;
        fields30 = makeFields("30");

        StructType schema31 = DataTypes.createStructType(fields31);
        StructType schema30 = DataTypes.createStructType(fields30);

        //splitRDDsByMonth and year
        JavaPairRDD<String,Iterable<Integer>> valuesMarch = values.filter(x -> x._1.split("_")[2].equals(year) && x._1.split("_")[1].equals("03"));
        JavaPairRDD<String,Iterable<Integer>> valuesApril = values.filter(x ->x._1.split("_")[2].equals(year) && x._1.split("_")[1].equals("04"));
        JavaPairRDD<String,Iterable<Integer>> valuesMay = values.filter(x -> x._1.split("_")[2].equals(year) && x._1.split("_")[1].equals("05"));

        JavaRDD<Row> marchRowRDD = valuesMarch.map(new get31ElemRow());
        JavaRDD<Row> aprilRowRDD = valuesApril.map(new get30ElemRow());
        JavaRDD<Row> mayRowRDD = valuesMay.map(new get31ElemRow());

        Dataset<Row> dfMarch = spark.createDataFrame(marchRowRDD, schema31);
        Dataset<Row> dfApril = spark.createDataFrame(aprilRowRDD, schema30);
        Dataset<Row> dfMay= spark.createDataFrame(mayRowRDD, schema31);

        List<Dataset<Row>> list = new ArrayList<>();
        list.add(dfMarch);
        list.add(dfApril);
        list.add(dfMay);

        return list;
    }

    private static List<StructField> makeFields(String month) {

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("city",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("one",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("two",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("three",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("four",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("five",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("six",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("seven",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("eight",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("nine",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("ten",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("eleven",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twelve",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("thirteen",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("fourteen",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("fifteen",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("sixteen",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("seventeen",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("eighteen",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("nineteen",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twenty",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twentyone",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twentytwo",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twentythree",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twentyfour",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twentyfive",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twentysix",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twentyseven",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twentyeight",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("twentynine",      DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("thirty",      DataTypes.IntegerType, true));

        if(month.equals("31")){
            fields.add(DataTypes.createStructField("thirtyone",      DataTypes.IntegerType, true));
        }

        return fields;

    }


    private static class get31ElemRow implements  Function<Tuple2<String,Iterable<Integer>>, Row>{

        @Override
        public Row call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {

            String city = stringIterableTuple2._1.split("_")[0];

            Integer[] elem = new Integer[31];
            int i=0;
            for(Integer v : stringIterableTuple2._2){
                elem[i]=v;
                i++;
            }

            return RowFactory.create(city,elem[0],elem[1],elem[2],elem[3],elem[4],elem[5],elem[6],elem[7],elem[8],elem[9],elem[10],
                    elem[11],elem[12],elem[13],elem[14],elem[15],elem[16],elem[17],elem[18],elem[19],elem[20],elem[21],elem[22]
                    ,elem[23],elem[24],elem[25],elem[26],elem[27],elem[28],elem[29],elem[30]);

        }

    }

    private static class get30ElemRow implements  Function<Tuple2<String,Iterable<Integer>>, Row>{

        @Override
        public Row call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {

            String city = stringIterableTuple2._1.split("_")[0];

            Integer[] elem = new Integer[31];
            int i=0;
            for(Integer v : stringIterableTuple2._2){
                elem[i]=v;
                i++;
            }

            return RowFactory.create(city,elem[0],elem[1],elem[2],elem[3],elem[4],elem[5],elem[6],elem[7],elem[8],elem[9],elem[10],
                    elem[11],elem[12],elem[13],elem[14],elem[15],elem[16],elem[17],elem[18],elem[19],elem[20],elem[21],elem[22]
                    ,elem[23],elem[24],elem[25],elem[26],elem[27],elem[28],elem[29]);

        }

    }
}
