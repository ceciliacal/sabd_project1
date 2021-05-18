package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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


        JavaRDD<String> lines_punti = sc.textFile(filePath_puntiSommTipologia);

        System.out.println("\nbefore map: ");
        System.out.println(lines_punti.take(5));

        //prendo header
        String[] header = lines_punti.map(line -> line.split(",")).first();
        String regione = header [0];

        System.out.println("regione: "+regione); // "tipologia" is printed

        //tupla che ha <regioni, denominazione_struttura>
        JavaRDD<Tuple2<String, String>> values = preprocessDataset(lines_punti);
       // preprocessDataset(lines_punti);

        /*
        per prendere singola colonna:
        JavaRDD<String> col_regioni= lines_punti.map(line -> line.split(",")[0]);

         */


        System.out.println("result: "+ values.take(5));


    }

    //preproc punti somministrazioni x regione (devo contare numero punti somm x ogni regione)
    private static JavaRDD<Tuple2<String, String>> preprocessDataset(JavaRDD<String> lines_punti) {

        //It is similar to map transformation; however, this transformation produces PairRDD, that is,
        // an RDD consisting of key and value pairs. This transformation is specific to Java RDDs

        //JavaRDD<Tuple2<String, String>> result = lines_punti.maptoPair ecc
        JavaRDD <Tuple2<String, String>> result = lines_punti.map(element -> {
                    String[] myFields = element.split(",");
                    String col_reg = myFields[0];
                    String col_denom = myFields[1];
                    return new Tuple2<>(col_reg,col_denom);
                }


        );

        //System.out.println("RDD Tuple2 "+ result.take(5));
        return result;
    }









/*
        JavaRDD<String> col_regioni= lines_punti.map(line -> line.split(",")[0]);
        JavaRDD<String> denom= lines_punti.map(line -> line.split(",")[1]);
        JavaRDD<Tuple2<String, String>> result2= new Tuple2<String, String>(col_regioni)


 */






    }



