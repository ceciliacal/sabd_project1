package sparkSqlQueries;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import queries.Query1;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;

import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.unix_timestamp;

public class SqlQuery1 {

    public static String filePath_puntiSommTipologia = Query1.filePath_puntiSommTipologia;
    public static String filePath_sommVacciniSummaryLatest = Query1.filePath_sommVacciniSummaryLatest;

    public static void main(String[] args) throws ParseException {

        computeQuery1();

    }

    public static void computeQuery1 (){

        SparkSession spark = SparkSession
                .builder()
                .appName("Query1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");


        Instant start = Instant.now();

        Dataset<Row> area_numCenters = preprocessNumCenters(spark);
        preprocessVaccinesSomm(spark, area_numCenters);

        Instant end = Instant.now();
        System.out.println("Tempo esecuzione query: " + Duration.between(start, end).toMillis() + "ms");
        spark.close();


    }

    public static Dataset<Row> preprocessNumCenters (SparkSession spark){

        // create schema for row with StructType
        StructType scheme = new StructType().add("reg","string").add("denomStrutt","string");

        //create dataframe from CSV file and apply scheme to it
        Dataset<Row> df_centers = spark.read().format("csv").option("header", "true").schema(scheme).csv("hdfs://hdfs-namenode:9000"+filePath_puntiSommTipologia);
        df_centers.show(3);

        // Register the DataFrame as a SQL temporary view
        df_centers.createOrReplaceTempView("query1");

        Dataset<Row> area_numCenters = spark.sql("SELECT reg, count(denomStrutt) as numCentri FROM query1 GROUP BY reg");
        area_numCenters.show();

        return area_numCenters;

    }



    public static Dataset<Row> preprocessVaccinesSomm (SparkSession spark, Dataset<Row> area_numCenters){

        // create schema for row with StructType
        StructType scheme = new StructType().add("date","date").add("area","string").add("numVacc","string");

        //create dataframe from CSV file and apply scheme to it
        Dataset<Row> df_somm = spark.read().format("csv").option("header", "true").schema(scheme).csv("hdfs://hdfs-namenode:9000"+filePath_sommVacciniSummaryLatest);
        df_somm.show(5);

        // Register the DataFrame as a SQL temporary view
        df_somm.createOrReplaceTempView("query1");



        Dataset<Row> dataset = spark.sql("SELECT area, date, numVacc FROM query1 WHERE DATE(date) >= DATE('2021-01-01') AND DATE(date) <= DATE('2021-05-31')");

        System.out.println("------------DATASET---------");
        dataset.show(3);
        System.out.println("------------DATASET---------");


        dataset.createOrReplaceTempView("initialdataset");
        System.out.println("------------QUERY RESULT---------1");
        area_numCenters.createOrReplaceTempView("regnumcenters");

        //faccio join con dataset precedente (dei punti somministrazione) e restituisco area,mese,numVacc,ngiorni(quanti giorni ha un certo mese),numCentri(num centri vaccinali per quella area)
        Dataset<Row> datasetSum = spark.sql("SELECT area, date_format(date, 'MMM') as mese , numVacc, day(last_day(date)) as ngiorni,  numCentri FROM initialDataset JOIN regNumCenters on initialDataset.area = regNumCenters.reg ");
        datasetSum.show(13);

        System.out.println("------------QUERY RESULT---------2");

        datasetSum.createOrReplaceTempView("regMonthVaccines");
        //calcolo valor medio delle vaccinazioni
        Dataset<Row> avgVacc = spark.sql("SELECT DISTINCT area, mese, SUM(numVacc/numCentri)/ngiorni as numVaccMedie FROM regMonthVaccines GROUP BY area, mese, ngiorni");

        avgVacc.show(105);

        System.out.println("------------QUERY RESULT---------3");


        return dataset;


    }
}
