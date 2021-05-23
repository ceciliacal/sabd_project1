package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;


import java.time.LocalDate;

import static queries.Query1.removeHeader;

public class Query2 {

    private static String filePath_sommVacciniLatest = "data/somministrazioni-vaccini-latest.csv";

    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Query1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> lines = sc.textFile(filePath_sommVacciniLatest);
        lines = removeHeader(lines);
        System.out.println("\nlines_punti: "+lines.take(5));

        JavaPairRDD<Tuple3<String, String, String>, Integer> month_area_age_numVacc = monthlyVaccines(lines);
        System.out.println("\nmonth_area_age_numVacc: "+month_area_age_numVacc.take(5));




    }


    //prelevo dati e creo le tuple <Key:<1 del mese, regione, fascia eta, >, Value:numVacciniDonne>
    public static JavaPairRDD<Tuple3<String, String, String>, Integer> monthlyVaccines(JavaRDD<String> lines) {

        LocalDate firstDay = LocalDate.parse("2021-02-01");
        LocalDate lastDay = LocalDate.parse("2021-05-31");

        JavaRDD<Tuple4<LocalDate, String, String, Integer>> rows = lines.map(row -> {

            String[] myFields = row.split(",");
            LocalDate col_date = LocalDate.parse(myFields[0]);
            String col_area = myFields[2];
            String col_age =  myFields[3];
            Integer col_numVacc =  Integer.parseInt(myFields[5]);

            return new Tuple4<>(col_date, col_area, col_age, col_numVacc);
        }).filter( line -> line._1().isAfter(firstDay) && line._1().isBefore(lastDay));

        JavaPairRDD<Tuple3<String, String, String>, Integer> res = rows.mapToPair( line ->
                new Tuple2<> ( new Tuple3<>(line._1().getMonth().toString(), line._2(), line._3()), line._4()));

        return res;
    }




}
