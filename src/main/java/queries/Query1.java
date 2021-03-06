package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import utils.CsvWriter;
import utils.Tuple2Comparator;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.*;


public class Query1 {

    public static String filePath_puntiSommTipologia = "/data/punti-somministrazione-tipologia.csv";
    public static String filePath_sommVacciniSummaryLatest = "/data/somministrazioni-vaccini-summary-latest.csv";
    private static Tuple2Comparator<String, Integer> tup2comp = new Tuple2Comparator<>(Comparator.<String>naturalOrder(), Comparator.<Integer>naturalOrder());

    public static void query1Main() {

        SparkSession spark = SparkSession
                .builder()
                .appName("Query1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");


        Instant start = Instant.now();

        /*
        ====================== lavoro con file "punti-somministrazione-tipologia" per calcolare numero di centri vaccinali per regione ===========================
         */

        JavaRDD<String> lines_puntiSommTipologia =
                spark.read().csv("hdfs://hdfs-namenode:9000"+filePath_puntiSommTipologia)
                .toJavaRDD().map(
                        row-> {
                          //  System.out.println( row.mkString(","));
                            return row.mkString(",");
                        }
                );
        JavaPairRDD<String, Integer> numVaccCenters = computeNumberVaccCentersPerArea(lines_puntiSommTipologia);

        /*
        ====================== lavoro con file somministrazioni-latest per calcolare quante vaccinazioni sono state fatte ======================
        ====================== in media giornalmente  da un generico centro vaccinale per ogni mese e per ogni regione    ======================
         */

        JavaRDD<String> lines_sommVacciniSummaryLatest =
                spark.read().csv("hdfs://hdfs-namenode:9000"+filePath_sommVacciniSummaryLatest)
                .toJavaRDD().map(   row-> {
                //    System.out.println( row.mkString(","));
                    return row.mkString(",");
                });
        List<Tuple2<Tuple2<String, Integer>, Integer>> avgVaccPerCenter = computeAvgVacc(lines_sommVacciniSummaryLatest, numVaccCenters);
        CsvWriter.writeQuery1(avgVaccPerCenter);


        Instant end = Instant.now();
        System.out.println("Tempo esecuzione query: " + Duration.between(start, end).toMillis() + "ms");

        spark.close();


    }


    public static List<Tuple2<Tuple2<String, Integer>, Integer>> computeAvgVacc(JavaRDD<String> lines, JavaPairRDD<String, Integer> numVaccCenters) {

        lines = removeHeader(lines);


        //faccio preprocessing e ottengo tupla es: [<ABR,FEB>,2000]
        JavaPairRDD<Tuple2<String, String>, Integer> area_month_totVacc = monthlyVaccinesPerArea(lines);
        System.out.println("\n\narea_month_totVacc PAIR KV: " + area_month_totVacc.take(30));

        //ora sommo tutti i tot vaccinazioni per quella chiave (quindi in quel mese)
        JavaPairRDD<Tuple2<String, String>, Integer> area_month_totVaccSum = area_month_totVacc.reduceByKey((x, y) -> x + y);
        System.out.println("area_month_totVaccSum SUM: " + area_month_totVaccSum.take(30));

        //ora cambio formato della tupla x fare il join con numVaccCenters (calcolato dal dataset punti-somministrione-tipologia)
        JavaPairRDD<String, Tuple2<String, Integer>> areaKey_month_totVaccSum = area_month_totVaccSum.mapToPair(line -> new Tuple2<>(line._1._1, new Tuple2<>(line._1._2, line._2)));
        System.out.println("area_month_totalSum: " + areaKey_month_totVaccSum.take(30));

        // join
        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> area_month_totVaccSum_numVaccCenters = areaKey_month_totVaccSum.join(numVaccCenters);
        System.out.println("area_month_totVaccSum_numVaccCenters JOIN : " + area_month_totVaccSum_numVaccCenters.take(30));

        //faccio la media del numero di vaccinazioni totali in un mese effettuate giornalmente da un generio centro vaccinale
        JavaPairRDD<Tuple2<String, String>, Integer> area_month_avgVaccPerCenter = area_month_totVaccSum_numVaccCenters.mapToPair(line -> {
            //prendo il numero di giorni di un mese
            String currentMonth_str = line._2._1._1;

            Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(currentMonth_str);
            Calendar cal = Calendar.getInstance();

            cal.setTime(date);
            cal.add(Calendar.MONTH, +1);
            int currentMonth = cal.get(Calendar.MONTH);

            YearMonth yearMonthObject = YearMonth.of(2021, currentMonth);
            int daysInMonth = yearMonthObject.lengthOfMonth();

            //ritorno tupla in cui effettuo divisione sia per numero di centri vaccinali sia per num giorni nel mese
            return new Tuple2<>(new Tuple2<>(line._1, line._2._1._1), Math.round(((line._2._1._2 / line._2._2)) / daysInMonth));
        });

        JavaPairRDD<Tuple2<String, Integer>, Integer> area_monthNumber_avgVaccPerCenter = convertMonthName(area_month_avgVaccPerCenter);

        List<Tuple2<Tuple2<String, Integer>, Integer>> resultList = area_monthNumber_avgVaccPerCenter.sortByKey(tup2comp, true, 1).collect();
        System.out.println("resultList : " + resultList);

        return resultList;

    }

    private static JavaPairRDD<Tuple2<String, Integer>, Integer> convertMonthName(JavaPairRDD<Tuple2<String, String>, Integer> area_month_avgVaccPerCenter) {

        JavaPairRDD<Tuple2<String, Integer>, Integer> result = area_month_avgVaccPerCenter.mapToPair(line -> {

            String currentMonth1_str = String.valueOf(line._1._2);
            Date currentMonth1 = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(currentMonth1_str);
            Calendar cal = Calendar.getInstance();
            cal.setTime(currentMonth1);
            int month1 = cal.get(Calendar.MONTH);
            System.out.println("\ncurrentMonth1: " + currentMonth1 + "   month1: " + month1);

            return new Tuple2<>(new Tuple2<>(line._1._1, month1), line._2);


        });

        return result;

    }

    public static JavaPairRDD<String, Integer> computeNumberVaccCentersPerArea(JavaRDD<String> lines) {

        lines = removeHeader(lines);
        System.out.println("\nlines_punti: " + lines.take(5));


        //creazione tupla con formato <regione, denominazione_struttura>
        JavaPairRDD<String, String> area_denomStrutt = centersPerArea(lines);
        System.out.println("\narea_denomStrutt: " + area_denomStrutt.take(5));


        //conto il num di strutture per regione. creo nuova tupla <regione, numero centri vaccinali>
        JavaPairRDD<String, Integer> countingVaccCenters = area_denomStrutt.mapToPair(line -> new Tuple2<>(line._1, 1));
        JavaPairRDD<String, Integer> numVaccCenters = countingVaccCenters.reduceByKey((x, y) -> x + y).distinct();
        System.out.println("\nNumero centri vaccinali: " + numVaccCenters.take(21));

        return numVaccCenters;

    }


    public static JavaRDD<String> removeHeader(JavaRDD<String> lines) {

        String[] header = lines.map(line -> line.split(",")).first();
        //System.out.println("\n\n header:"+ Arrays.toString(header2));
        JavaRDD<String> lines_noHeader = lines.filter(line -> line.contains(header[0]) == false);

        return lines_noHeader;

    }


    //preprocessamento somministrazioni vaccini summary latest, per prendere le colonne area, data somm, totale
    //da mettere in una tupla x processare i dati dopo
    public static JavaPairRDD<Tuple2<String, String>, Integer> monthlyVaccinesPerArea(JavaRDD<String> lines) {

        LocalDate firstDay = LocalDate.parse("2021-01-01");
        LocalDate lastDay = LocalDate.parse("2021-05-31");

        JavaRDD<Tuple3<String, LocalDate, Integer>> resultWithLocalDate = lines.map(row -> {

            String[] myFields = row.split(",");
            LocalDate col_date = LocalDate.parse(myFields[0]);
            String col_area = myFields[1];
            Integer col_tot = Integer.parseInt(myFields[2]);

            return new Tuple3<>(col_area, col_date, col_tot);
        }).filter(line -> line._2().isAfter(firstDay) && line._2().isBefore(lastDay));

        JavaPairRDD<Tuple2<String, String>, Integer> res = resultWithLocalDate.mapToPair(line ->
                new Tuple2<>(new Tuple2<>(line._1(), line._2().getMonth().toString()), line._3()));

        return res;
    }


    //preprocessamento punti somministrazioni x regione
    public static JavaPairRDD<String, String> centersPerArea(JavaRDD<String> lines_punti) {

        JavaPairRDD<String, String> result = lines_punti.mapToPair(element -> {
                    String[] myFields = element.split(",");
                    String col_reg = myFields[0];
                    String col_denom = myFields[1];
                    return new Tuple2<>(col_reg, col_denom);
                }
        );

        //System.out.println("RDD Tuple2 "+ result.take(5));
        return result;
    }


}



