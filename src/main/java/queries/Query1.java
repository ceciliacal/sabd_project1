package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Query1 {

    private static String filePath_puntiSommTipologia = "data/punti-somministrazione-tipologia.csv";
    private static String filePath_sommVacciniSummaryLatest = "data/somministrazioni-vaccini-summary-latest.csv";

    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Query1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("ciao");
        sc.setLogLevel("ERROR");

        JavaRDD<String> lines_puntiSommTipologia = sc.textFile(filePath_puntiSommTipologia);
        System.out.println(lines_puntiSommTipologia.take(5));

        String[] header = lines_puntiSommTipologia.map(line -> line.split(",")).first();
        String regione = header [0];
        String denom_struttura = header [1];
        System.out.println(regione); // "tipologia" is printed
        System.out.println(denom_struttura);
        System.out.println("cacccca");








    }






}
