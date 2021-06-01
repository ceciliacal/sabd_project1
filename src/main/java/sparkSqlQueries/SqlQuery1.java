package sparkSqlQueries;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import queries.Query1;

import java.text.ParseException;

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
                .appName("sqlQuery1").master("local[*]")
                .getOrCreate();

        Dataset<Row> area_numCenters = preprocessNumCenters(spark);
        preprocessVaccinesSomm(spark, area_numCenters);


    }

    public static Dataset<Row> preprocessNumCenters (SparkSession spark){

        // create schema for row with StructType
        StructType scheme = new StructType().add("reg","string").add("denomStrutt","string");
        //create dataframe from CSV file and apply scheme to it
        Dataset<Row> df_centers = spark.read().format("csv").option("header", "true").schema(scheme).csv(filePath_puntiSommTipologia);

        //System.out.println("\n------DF = "+ df_centers.getRows(3,2));
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
        Dataset<Row> df_somm = spark.read().format("csv").option("header", "true").schema(scheme).csv(filePath_sommVacciniSummaryLatest);

        System.out.println("\nPROVAAAAA");
        df_somm.show(5);


        // Register the DataFrame as a SQL temporary view
        df_somm.createOrReplaceTempView("query1");



        Dataset<Row> dataset = spark.sql("SELECT area, date, numVacc FROM query1 WHERE DATE(date) > DATE('2021-1-31')");

        System.out.println("------------DATASET---------");
        dataset.show(3);
        System.out.println("------------DATASET---------");


        dataset.createOrReplaceTempView("initialDataset");
        System.out.println("------------DATASETSUM---------1");
        //Dataset<Row> datasetSum = spark.sql("SELECT area, MONTH(date) as month, SUM(numVacc) as sum FROM initialDataset GROUP BY area");
        Dataset<Row> datasetSum = spark.sql("SELECT area, date_format(date, 'MMM') as mese , numVacc FROM initialDataset");
        datasetSum.show(13);
        datasetSum.createOrReplaceTempView("regMonthVaccines");
        area_numCenters.createOrReplaceTempView("regNumCenters");
        //Dataset<Row> areaMonthVaccines = spark.sql("SELECT area, mese, SUM(numVacc) as sum FROM areaMonthVaccines "+" GROUP BY area, mese");
        Dataset<Row> regMonthVaccines = spark.sql("SELECT area, mese,  SUM(numVacc/numCentri) as monthlyVaccPerCenters FROM regMonthVaccines JOIN regNumCenters on regMonthVaccines.area = regNumCenters.reg GROUP BY area, mese");

        regMonthVaccines.createOrReplaceTempView("regMonthVaccines");



        regMonthVaccines.show(13);
        System.out.println("------------DATASETSUM---------2");


        /*
        df_somm.withColumn("month", from_unixtime(unix_timestamp(dataset.col("date"), "yy/MM/dd hh:mm"), "MMMMM"));
        System.out.println("---------------------");
        dataset.show(3);
        System.out.println("---------------------");

         */






        return dataset;





    }
}
