package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;


import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import static queries.Query1.removeHeader;

public class Query2 {

    private static String filePath_sommVacciniLatest = "data/somministrazioni-vaccini-latest.csv";
    private static Tuple3Comparator<LocalDate, String, String> tupComp_date =  new Tuple3Comparator<LocalDate, String, String>(Comparator.<LocalDate>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());
    private static Tuple3Comparator<String , String, String> tupComp =  new Tuple3Comparator<String, String, String>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());
    private static Tuple2Comparator<Integer, String> tup2comp = new Tuple2Comparator<>(Comparator.<Integer>naturalOrder(), Comparator.<String>naturalOrder());


    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Query2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> lines = sc.textFile(filePath_sommVacciniLatest);
        lines = removeHeader(lines);
        //System.out.println("\nlines_punti: "+lines.take(5));

        JavaPairRDD<Tuple3<LocalDate, String, String>, Integer> month_area_age_numVacc_byBrand = monthlyVaccines(lines);
        System.out.println("\nmonth_area_age_numVacc_byBrand: "+month_area_age_numVacc_byBrand.take(5));

        //considerare categorie per cui in quel mese vengono registrati almeno 2 giorni di campagna vaccinale
        /*
        esempio: in molise a febbraio non hanno vaccinato nessuna femmina di 16-19 anni, quindi non lo considero

         */

        //TODO : ORDINAMENTO
        JavaPairRDD<Tuple3<LocalDate, String, String>, Integer> bho = month_area_age_numVacc_byBrand.sortByKey(tupComp_date,true,1 );
        System.out.println("\n----- bho: "+bho.take(30));



        //TOLGO DISTINZIONE PER FORNITORE: sommo tutti i vaccini effettuati a donne di stessa fascia anagrafica, nello stesso giorno e nella stessa regione
       JavaPairRDD<Tuple3<String, String, String>, Tuple2 <LocalDate, Integer>> month_area_age_numVacc = bho
               .reduceByKey((x,y) -> x+y)
               .mapToPair(line -> new Tuple2<>( new Tuple3<>(line._1._1().getMonth().toString(), line._1._2(), line._1._3()) , new Tuple2<>(line._1._1(), line._2) ));
               //.filter(x -> x._1._1().toString().equals("2021-03-04") && x._1._2().equals("TOS") && x._1._3().equals("90+"));
        System.out.println("\nSOMMA x FORNITORE: month_area_age_numVacc: "+month_area_age_numVacc.take(10));


        //System.out.println("\nprovacount FEB,MAR,40-49: "+month_area_age_numVacc.filter(x -> x._1._1().equals("FEBRUARY") && x._1._2().equals("MAR") && x._1._3().equals("40-49")).take(27));


        //devo guardare mese e numVacc, se ci sono più 2 di giorni con num vacc >0 allora lo tengo
        //mi servono due mapTOpAIR diverse da cui poi prendo i dati
        //quindi ora devo fare una tupla che contiene il count

        //devo fare dictionary e faccio .contains per vedere se lo contiene

        /*
        devo fare un dizionario con key recente e value = num di giorni in cui so stati fatti vaccini in quel mese
        poi faccio filter e controllo se dictionary.contains(value >=2) == true
         */

        JavaPairRDD<Tuple3<String, String, String>, Integer> provaCount = month_area_age_numVacc
                .filter(x -> x._2._2() > 0)     // prendo righe dove numero di vaccini per quel giorno è > 0
                .mapToPair( line -> new Tuple2<>( new Tuple3<>(line._1._1(), line._1._2(), line._1._3()),1))
                .reduceByKey((x,y) -> x+y)
                .filter( x -> x._2 >= 2);       //check errore!!!!!!!!


        System.out.println("\nprovacount: "+provaCount.take(10));
        /*
        System.out.println("\nprovacount SIZE: "+provaCount.count());
        System.out.println("\n provacount FEB,MOL,16-19 camp:"+provaCount.filter(x -> x._1._1().equals("FEBRUARY") && x._1._2().equals("MOL") && x._1._3().equals("16-19") ).take(27));
        System.out.println("\nprovacount FEB,EMR,30-39  "+provaCount.filter(x -> x._1._1().equals("FEBRUARY") && x._1._2().equals("EMR") && x._1._3().equals("30-39") ).take(28));


         */


        Map<Tuple3<String, String, String>, Long> mappa = provaCount.countByKey();
        System.out.println("\nsize mappa: "+mappa.size());

        month_area_age_numVacc = month_area_age_numVacc.filter(x -> mappa.containsKey(x._1));

        System.out.println("\nFILTERED month_area_age_numVacc: "+month_area_age_numVacc.take(10));

        System.out.println("\nCOUNT month_area_age_numVacc: "+month_area_age_numVacc.countByKey().size());
/*
        System.out.println("\nFILTERED - PROVA FEB,EMR,30-39 camp: month_area_age_numVacc: "+month_area_age_numVacc.filter(x -> x._1._1().equals("FEBRUARY") && x._1._2().equals("EMR") && x._1._3().equals("30-39") && x._2()._2 == 0).take(27));
        System.out.println("\nFILTERED - PROVA FEB,MOL,16-19 camp: month_area_age_numVacc: "+month_area_age_numVacc.filter(x -> x._1._1().equals("FEBRUARY") && x._1._2().equals("MOL") && x._1._3().equals("16-19") ).take(27));
        System.out.println("\nFILTERED - PROVA 16FEB,BAS,16-19 camp: month_area_age_numVacc: "+month_area_age_numVacc.filter(x -> x._1._1().equals("FEBRUARY") && x._1._2().equals("BAS") && x._1._3().equals("16-19") && x._2()._2 == 0 ).take(27));


         */

        System.out.println("\n====== VEDO SE CE QUALCHE NEGATIVO "+month_area_age_numVacc.filter(x->x._2()._2 <= 0).take(30));


        System.out.println("\nmonth_area_age_numVacc COUNT: "+month_area_age_numVacc.count());
        System.out.println("\nmonth_area_age_numVacc COUNT BY KEY: "+month_area_age_numVacc.distinct().count());
        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> reg = regress(month_area_age_numVacc);
        System.out.println("\nreg COUNT: "+reg.count());

        //TODO: DEVO ORDINARE LE DATE !!!!!!??????????
        //JavaPairRDD<Tuple3<String, String, String>, Tuple3<LocalDate, Integer, Integer>> reg2 = reg.reduceByKey((x,y) -> x.append(y) );
        //posso usare come key quella in bho e faccio la add di data e num vacc (così sono ordinate), poi creo nuova tupla che ha come key mese,reg,fascia e come value il valore atteso
        //oppure un java pair con una tupla4 come key in cui ci metto pure il mese

        reg = reg.reduceByKey((x,y) -> {
            x.append(y);
            return x;
        });

        //System.out.println("\nreg prova test: "+month_area_age_numVacc.filter(x -> x._1._1().equals("FEBRUARY") && x._1._2().equals("EMR") && x._1._3().equals("30-39")).sort);


        JavaPairRDD<Tuple3<String, String, String>, Integer> pred = reg.mapToPair( line -> {
            double predictedNumVacc;
            String currentMonth_str = line._1._1();

            Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(currentMonth_str);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.MONTH, +2);
            int currentMonth = cal.get(Calendar.MONTH);

            String predictionDay_str = "2021-0"+currentMonth+"-01";
            Date predictionDay=new SimpleDateFormat("yyyy-MM-dd").parse(predictionDay_str);

            predictedNumVacc = line._2.predict((double)predictionDay.getTime());

            return new Tuple2<>(new Tuple3<>(predictionDay_str, line._1._2(), line._1._3() ), (int)predictedNumVacc);

        });

        JavaPairRDD<Tuple3<String, String, String>, Integer> sortedPredictions = pred.sortByKey(tupComp,true,1 );
        System.out.println("\nsortedPredictions: "+sortedPredictions.take(10));



/*
        System.out.println("\npred NEGATIVI count: "+pred.filter(x -> x._2 <0).count());
        System.out.println("\npred NEGATIVI: "+pred.filter(x -> x._2 <0).take(39));
        System.out.println("\nFILTERED - PROVA MAY,LAZ,80-89 camp: month_area_age_numVacc: "+month_area_age_numVacc.filter(x -> x._1._1().equals("MAY") && x._1._2().equals("LAZ") && x._1._3().equals("80-89") ).take(31));


 */
        JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, String>> rank = sortedPredictions
                .mapToPair( line -> new Tuple2<>(line._2, line._1))
                .sortByKey(false)
                .mapToPair(line -> new Tuple2<>(new Tuple2<>(line._2._1(), line._2._3()),  new Tuple2<>(line._1, line._2._2())));
                //.sortByKey(tup2comp,false,1);
                //map to pair e metto key giusta
        //poi faccio reduce by key e comparo con Comparatore i value che gli passo


        System.out.println("\nrank: "+rank.take(10));









    }



    //month_area_age_datenumVacc
    public static JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> regress(JavaPairRDD<Tuple3<String, String, String>, Tuple2 <LocalDate, Integer>> lines){

        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> reg =  lines.mapToPair(line -> {
            SimpleRegression regression = new SimpleRegression();
            LocalDate myLocalDate = line._2._1();
            Instant instant = myLocalDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
            Date myDate = Date.from(instant);
            regression.addData((double) myDate.getTime(), (double) line._2._2());
                    return new Tuple2<>(line._1, regression);
                }
        );

        return reg;
    }




    //prelevo dati e creo le tuple <Key:<1 del mese, regione, fascia eta, >, Value:numVacciniDonne>
    public static JavaPairRDD<Tuple3<LocalDate, String, String>, Integer> monthlyVaccines(JavaRDD<String> lines) {

        LocalDate firstDay = LocalDate.parse("2021-01-31");
        LocalDate lastDay = LocalDate.parse("2021-06-01");

        JavaRDD<Tuple4<LocalDate, String, String, Integer>> rows = lines.map(row -> {

            String[] myFields = row.split(",");
            LocalDate col_date = LocalDate.parse(myFields[0]);
            String col_area = myFields[2];
            String col_age =  myFields[3];
            Integer col_numVacc =  Integer.parseInt(myFields[5]);

            return new Tuple4<>(col_date, col_area, col_age, col_numVacc);
        }).filter( line -> line._1().isAfter(firstDay) && line._1().isBefore(lastDay));

        JavaPairRDD<Tuple3<LocalDate, String, String>, Integer> res = rows.mapToPair( line ->
                new Tuple2<> ( new Tuple3<>(line._1(), line._2(), line._3()), line._4()));

        return res;
    }




}
