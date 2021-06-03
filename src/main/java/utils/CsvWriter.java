package utils;

import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

import static org.spark_project.guava.collect.Iterables.limit;

public class CsvWriter {

    public static String filePath_resQuery2 = "results/query2_result.csv";
    public static String filePath_resQuery1 = "results/query1_result.csv";


    public static void writeQuery2 (List<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>>> list) {

        try (PrintWriter writer = new PrintWriter(new File(filePath_resQuery2))) {

            StringBuilder sb = new StringBuilder();
            sb.append("Data, Fascia Anagrafica");
            sb.append(';');

            sb.append("TOP 5 (Numero vaccinazioni previste, Regione)");
            sb.append('\n');




            for (Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>> elem : list){
                Iterable<Tuple2<Integer, String>> value = elem._2;
                //System.out.println("\nkey: "+ elem._1());
                sb.append(elem._1());
                sb.append(';');
                sb.append(limit(value, 5));
                sb.append('\n');


                //System.out.println("\nelem: "+limit(value, 5));
                //System.out.println("#####\n\n");


            }

            writer.write(sb.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void writeQuery1(List<Tuple2<Tuple2<String,Integer>,  Integer>> list) {

        try (PrintWriter writer = new PrintWriter(new File(filePath_resQuery1))) {

            StringBuilder sb = new StringBuilder();
            sb.append("Regione, Mese");
            sb.append(';');

            sb.append("Numero medio vaccinazioni");
            sb.append('\n');




            for (Tuple2<Tuple2<String,Integer>, Integer> elem : list){

                Integer value = elem._2;
                //Tuple2<String,Integer> bho = elem._1;

                String month = ConvertIntToMonth.convert(elem._1._2);

                //System.out.println("\nkey: "+ elem._1());
                sb.append("("+elem._1._1+","+month+")");
                sb.append(';');
                sb.append(value);
                sb.append('\n');


            }

            writer.write(sb.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


}
