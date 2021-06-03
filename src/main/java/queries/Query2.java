package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.CsvWriter;
import utils.Tuple2Comparator;
import utils.Tuple3Comparator;

import static org.spark_project.guava.collect.Iterables.limit;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import static queries.Query1.removeHeader;

public class Query2 {

	private static String filePath_sommVacciniLatest = "data/somministrazioni-vaccini-latest.csv";
	private static Tuple3Comparator<LocalDate, String, String> tupComp_date =  new Tuple3Comparator<LocalDate, String, String>(Comparator.<LocalDate>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());
	private static Tuple3Comparator<String , String, String> tupComp =  new Tuple3Comparator<String, String, String>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());
	private static Tuple2Comparator<String, String> tup2comp = new Tuple2Comparator<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());


	public static void query2Main() {

		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("Query2");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		Instant start = Instant.now();

		JavaRDD<String> lines = sc.textFile(filePath_sommVacciniLatest);
		lines = removeHeader(lines);
		//System.out.println("\nlines_punti: "+lines.take(5));

		JavaPairRDD<Tuple3<LocalDate, String, String>, Integer> month_area_age_numVacc_byBrand = monthlyVaccines(lines);
		System.out.println("\nmonth_area_age_numVacc_byBrand: "+month_area_age_numVacc_byBrand.take(5));


		//rimuovo distinzione per fornitore: sommo tutti i vaccini effettuati a donne di stessa fascia anagrafica, nello stesso giorno e nella stessa regione
		JavaPairRDD<Tuple3<String, String, String>, Tuple2 <LocalDate, Integer>> month_area_age_numVacc = month_area_age_numVacc_byBrand
				.reduceByKey((x,y) -> x+y)
				.mapToPair(line -> new Tuple2<>( new Tuple3<>(line._1._1().getMonth().toString(), line._1._2(), line._1._3()) , new Tuple2<>(line._1._1(), line._2) ));

		System.out.println("\nSOMMA x FORNITORE: month_area_age_numVacc: "+month_area_age_numVacc.take(10));

		//considerare categorie per cui in quel mese vengono registrati almeno 2 giorni di campagna vaccinale
		//esempio: in molise a febbraio non hanno vaccinato nessuna femmina di 16-19 anni, quindi non lo considero
		month_area_age_numVacc = checkMoreThan2DaysPerMonth(month_area_age_numVacc);

		//calcolo le predizioni (qui sono ordinate secondo la key <1giornoMese, Regione,Fascia> es: (2021-03-01,CAL,12-19), (2021-03-01,CAL,20-29), ecc...
		JavaPairRDD<Tuple3<String, String, String>, Integer> predictions = calculateSortedPredictions(month_area_age_numVacc);
		System.out.println("\nsortedPredictions: "+predictions.filter(x -> x._1._2().equals("CAL")).take(30));

		//ordino tra loro le chiavi e il numero di vaccinazioni predette in base alla chiave
		List<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>>> rank = sortPredictionsByKey(predictions);

		//creo top5
		CsvWriter.writeQuery2(rank);

		/*
		for (Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>> elem : rank){
			Iterable<Tuple2<Integer, String>> value = elem._2;
			System.out.println("\nkey: "+ elem._1());

			System.out.println("\nelem: "+limit(value, 5));
			System.out.println("#####\n\n");


		}

		 */

		System.out.println("\nrank: "+rank);

		Instant end = Instant.now();
		System.out.println("Tempo esecuzione query: " + Duration.between(start, end).toMillis() + "ms");

		sc.close();


	}


	public static List<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>>>  sortPredictionsByKey (JavaPairRDD<Tuple3<String, String, String>, Integer> predictions){

		List<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>>> rank = predictions
				.mapToPair( line -> new Tuple2<>(line._2, line._1))
				.sortByKey(false)
				.mapToPair(line -> new Tuple2<>(new Tuple2<>(line._2._1(), line._2._3()),  new Tuple2<>(line._1, line._2._2())))
				.groupByKey()
				.sortByKey(tup2comp, true, 1)
				.collect();

		return rank;


	}
	public static JavaPairRDD<Tuple3<String, String, String>, Integer> calculateSortedPredictions (JavaPairRDD<Tuple3<String, String, String>, Tuple2 <LocalDate, Integer>> month_area_age_numVacc){

		//applico regressione sui dati
		JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> month_area_age_SimpleReg = calculateRegression(month_area_age_numVacc);
		System.out.println("\nreg COUNT: "+month_area_age_SimpleReg.count());

		//faccio append di tutti gli oggetti SimpleRegression con stessa chiave
		month_area_age_SimpleReg = month_area_age_SimpleReg.reduceByKey((x,y) -> {
			x.append(y);
			return x;
		});

		//faccio la predizione
		JavaPairRDD<Tuple3<String, String, String>, Integer> result = predictFirstDayOfNextMonth(month_area_age_SimpleReg);
		return result;


	}

	public static JavaPairRDD<Tuple3<String, String, String>, Integer>  predictFirstDayOfNextMonth(JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> month_area_age_SimpleReg){

		//faccio predizione del primo giorno del mese successivo facendo
		// una predict sul SimpleRegression di una certa chiave (<mese, reg, fascia>) su una "x" che è il numero
		// successivo rispetto al numero dell'ultimo giorno di quel mese (riportato nella chiave e nel valore di reg)
		//ad es. Maggio ha come ultimo giorno 31, faccio la predict su 32 (come se fosse il 1 giugno, ossia il giorno dopo il 31 maggio)
		// JavaPairRDD<Tuple3<mese, reg, fascia>, SimpleRegression> reg

		JavaPairRDD<Tuple3<String, String, String>, Integer> month_area_age_predictedNumVacc = month_area_age_SimpleReg.mapToPair( line -> {
			double predictedNumVacc;
			String currentMonth_str = line._1._1(); //prendo il mese che è scritto come stringa

			//ottengo mese corrente in formato Date
			Date currentMonth = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(currentMonth_str);

			Calendar cal = Calendar.getInstance();
			cal.setTime(currentMonth);

			//prendo il numero successivo rispetto all'ultimo giorno del mese corrente
			int lastDay = cal.getActualMaximum(Calendar.DATE);	//ultimo giorno di currentMonth
			int dayToPredict = lastDay + 1 ;

			cal.add(Calendar.MONTH, +2);
			int monthToPredict = cal.get(Calendar.MONTH);   //mese successivo (per la predizione)

			String predictionDay_str = "2021-0"+monthToPredict+"-01";   //primo giorno del mese successivo

			predictedNumVacc = line._2.predict(dayToPredict);

			return new Tuple2<>(new Tuple3<>(predictionDay_str, line._1._2(), line._1._3() ), (int)predictedNumVacc);

		});

		JavaPairRDD<Tuple3<String, String, String>, Integer> sortedPredictions = month_area_age_predictedNumVacc.sortByKey(tupComp,true,1 );

		return sortedPredictions;

	}

	public static JavaPairRDD<Tuple3<String, String, String>, Tuple2<LocalDate, Integer>> checkMoreThan2DaysPerMonth (JavaPairRDD<Tuple3<String, String, String>, Tuple2 <LocalDate, Integer>> month_area_age_numVacc ){

		//creo un dizionario con una specifica key <Mese,Regione,Fascia> e come value il numero di giorni in cui so stati fatti
		// vaccini in quel mese. Poi faccio filter su month_area_age_numVacc e controllo se la sua key è mantenuta nel dictionary

		JavaPairRDD<Tuple3<String, String, String>, Integer> countNumVacc = month_area_age_numVacc
				.filter(x -> x._2._2() > 0)     // prendo righe dove numero di vaccini per quel giorno è > 0
				.mapToPair( line -> new Tuple2<>( new Tuple3<>(line._1._1(), line._1._2(), line._1._3()),1))
				.reduceByKey((x,y) -> x+y)
				.filter( x -> x._2 >= 2);       //check errore!!!!!!!!


		System.out.println("\nprovacount: "+countNumVacc.take(10));

		//conto il numero di giorni di campagna vaccinale in un certo Mese,Regione,Fascia e li inserisco in una mappa
		Map<Tuple3<String, String, String>, Long> myDictionary = countNumVacc.countByKey();
		System.out.println("\nsize mappa: "+myDictionary.size());

		//filtro javaPairRDD e tengo solo quelli con chiave che sta nel dictionary
		//ossia le chiavi Mese,Regione,Fascia con almeno 2 giorni di campagna vaccinale al mese
		month_area_age_numVacc = month_area_age_numVacc.filter(x -> myDictionary.containsKey(x._1));

		System.out.println("\nFILTERED month_area_age_numVacc: "+month_area_age_numVacc.take(10));

		System.out.println("\nCOUNT month_area_age_numVacc: "+month_area_age_numVacc.countByKey().size());

		return month_area_age_numVacc;


	}



	//month_area_age_(date_numVacc)
	//prendo input e creo nuova coppia con key:mese,regione,fascia e come value l'oggetto Simple regression per poi fare successivamente l'append fra ogni
	//oggetto simple regression avente stessa chiaves
	public static JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> calculateRegression(JavaPairRDD<Tuple3<String, String, String>, Tuple2 <LocalDate, Integer>> lines){

		JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> reg =  lines.mapToPair(line -> {

					SimpleRegression regression = new SimpleRegression();
					LocalDate myLocalDate = line._2._1();
					Instant instant = myLocalDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
					Date myDate = Date.from(instant);
					int day = myLocalDate.getDayOfMonth();

					//addData -> data = (giorno, numero di vaccini fatti quel giorno)
					regression.addData((double) day, (double) line._2._2());
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
