package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Query1 {

    private static String filePath_puntiSommTipologia = "data/punti-somministrazione-tipologia.csv";
    private static String filePath_sommVacciniSummaryLatest = "data/somministrazioni-vaccini-summary-latest.csv";

    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Query1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");


        Instant start = Instant.now();

        JavaRDD<String> lines_punti = sc.textFile(filePath_puntiSommTipologia);
        lines_punti = removeHeader(lines_punti);
        System.out.println("\nlines_punti: "+lines_punti.take(5));


        //tupla che ha <regioni, denominazione_struttura>
        JavaPairRDD<String, String> area_denomStrutt = centersPerArea(lines_punti);
        System.out.println("\narea_denomStrutt: "+area_denomStrutt.take(5));


        // poi contare il num di strutture per regione
        /* cioè devo creare nuovo RDD dove la chiave è la regione e il value è il numero di strutture
        appartenenti a quella regione
         */


        JavaPairRDD<String, Integer> countingVaccCenters = area_denomStrutt.mapToPair( line -> new Tuple2<>(line._1, 1));
        JavaPairRDD<String, Integer> numVaccCenters = countingVaccCenters.reduceByKey((x,y) -> x+y).distinct();
        System.out.println("\nNumero centri vaccinali: "+numVaccCenters.take(21));



        /*
        ====================== inizio 2 parte della query (con file somministrazioni-latest) ===========================
         */

        JavaRDD<String> lines_somm = sc.textFile(filePath_sommVacciniSummaryLatest);
        lines_somm = removeHeader(lines_somm);


        //qui ho KV es: [<ABR,FEB>,2000]
        JavaPairRDD<Tuple2<String, String>, Integer> area_month_totVacc = monthlyVaccinesPerArea(lines_somm);
        System.out.println("\n\narea_month_totVacc PAIR KV: "+area_month_totVacc.take(30));

        //ora sommo tutti i tot vaccinazioni per quella chiave (quindi in quel mese)
        JavaPairRDD<Tuple2<String, String>, Integer> area_month_totVaccSum = area_month_totVacc.reduceByKey( (x,y) -> x +y);
        System.out.println("area_month_totVaccSum SUM: "+area_month_totVaccSum.take(30));

        //ora cambio formato della tupla x fare il join

        JavaPairRDD<String, Tuple2<String, Integer>> areaKey_month_totVaccSum = area_month_totVaccSum.mapToPair(line -> new Tuple2<>(line._1._1, new Tuple2<>(line._1._2, line._2)));
        System.out.println("area_month_totalSum: "+areaKey_month_totVaccSum.take(30));

        //ora faccio join

        JavaPairRDD<String, Tuple2< Tuple2<String, Integer>, Integer>> area_month_totVaccSum_numVaccCenters = areaKey_month_totVaccSum.join(numVaccCenters);
        System.out.println("area_month_totVaccSum_numVaccCenters JOIN : "+area_month_totVaccSum_numVaccCenters.take(30));

        //faccio la media del numero di vaccinazioni totali in un mese effettuate da un generio centro vaccinale
        JavaPairRDD<Tuple2<String,String>,  Integer> area_month_avgVaccPerCenter = area_month_totVaccSum_numVaccCenters.mapToPair( line -> new Tuple2<>(new Tuple2<>(line._1, line._2._1._1) , line._2._1._2/line._2._2));
        System.out.println("area_month_avgVaccPerCenter : "+area_month_avgVaccPerCenter.take(30));

        Instant end = Instant.now();
        System.out.println("Tempo esecuzione query: " + Duration.between(start, end).toMillis() + "ms");

        sc.close();



    }


    /*
    per prendere singola colonna:
        JavaRDD<String> col_regioni= lines_punti.map(line -> line.split(",")[0]);
     */


    public static JavaRDD<String> removeHeader(JavaRDD<String> lines) {

        String[] header = lines.map(line -> line.split(",")).first();
        //System.out.println("\n\n header:"+ Arrays.toString(header2));
        JavaRDD<String> lines_noHeader  = lines.filter(line -> line.contains(header[0]) == false);

        return lines_noHeader;

    }



    //preproc somministrazioni vaccini summary latest, per prendere le colonne area, data somm, totale
    //da mettere in una tupla x processare i dati dopo
    public static JavaPairRDD<Tuple2<String, String>, Integer> monthlyVaccinesPerArea(JavaRDD<String> lines) {

        LocalDate firstDay = LocalDate.parse("2021-01-01");
        LocalDate lastDay = LocalDate.parse("2021-05-31");

        JavaRDD<Tuple3<String, LocalDate, Integer>> resultWithLocalDate = lines.map( row -> {

            String[] myFields = row.split(",");
            LocalDate col_date = LocalDate.parse(myFields[0]);
            String col_area = myFields[1];
            Integer col_tot =  Integer.parseInt(myFields[2]);

            return new Tuple3<>(col_area, col_date, col_tot);
        }).filter( line -> line._2().isAfter(firstDay) && line._2().isBefore(lastDay));

        JavaPairRDD<Tuple2<String, String>, Integer> res = resultWithLocalDate.mapToPair( line ->
                new Tuple2<> ( new Tuple2<>(line._1(), line._2().getMonth().toString() ), line._3()));

        return res;
    }


    //preproc punti somministrazioni x regione (devo contare numero punti somm x ogni regione)
    public static JavaPairRDD<String, String> centersPerArea(JavaRDD<String> lines_punti) {

        JavaPairRDD <String, String> result = lines_punti.mapToPair(element -> {
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



