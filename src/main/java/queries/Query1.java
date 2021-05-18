package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;


public class Query1 {

    private static String filePath_puntiSommTipologia = "data/punti-somministrazione-tipologia.csv";
    private static String filePath_sommVacciniSummaryLatest = "data/somministrazioni-vaccini-summary-latest.csv";

    public static <iter, tr> void main(String[] args){

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
        //lines_punti = lines_punti.filter(line -> !line.equals(header));
        String regione = header [0];

        System.out.println("regione: "+regione);

        //tupla che ha <regioni, denominazione_struttura>
        JavaRDD<Tuple2<String, String>> area_denomStrutt = preprocessDataset(lines_punti);

        System.out.println("area_denomStrutt: "+area_denomStrutt.take(5));


        /*
        per prendere singola colonna:
        JavaRDD<String> col_regioni= lines_punti.map(line -> line.split(",")[0]);

         */

        // adesso devo eliminare la prima riga


                /*(idx, iter) ->{
              if (idx == 0) iter.drop(1) else iter;
        });

                 */

        // poi contare il num di strutture per regione
        /* cioè devo creare nuovo RDD dove la chiave è la regione e il value è il numero di strutture
        appartenenti a quella regione
         */

        JavaPairRDD.fromJavaRDD(area_denomStrutt);
        Map<String, Long> counts = JavaPairRDD.fromJavaRDD(area_denomStrutt).countByKey();
        System.out.println("counts"+counts);



    }

    //preproc punti somministrazioni x regione (devo contare numero punti somm x ogni regione)
    private static JavaRDD<Tuple2<String, String>> preprocessDataset(JavaRDD<String> lines_punti) {

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


}



