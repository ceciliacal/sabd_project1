package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CsvWriter {

    public static String filePath_resQuery2 = "/results/query2_result.csv";
    public static String filePath_resQuery1 = "/results/query1_result.csv";


    public static void writeQuery2 (List<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>>> list) {

        //importo files di output su hdfs
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hdfs-namenode:9000");
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
            Path hdfsWritePath = new Path("hdfs://hdfs-namenode:9000"+filePath_resQuery2);
            FSDataOutputStream fsDataOutputStream = null;
            fsDataOutputStream = hdfs.create(hdfsWritePath,true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder();
            sb.append("Data, Fascia Anagrafica");
            sb.append(';');

            sb.append("TOP 5 (Numero vaccinazioni previste, Regione)");
            sb.append('\n');




            for (Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>> elem : list){
                Iterable<Tuple2<Integer, String>> value = elem._2;
                sb.append(elem._1());
                sb.append(';');
                int i = 0;
                sb.append("[");
                //top5
                for (Tuple2<Integer, String> integerStringTuple2 : value) {
                    if(i==5)
                        break;
                    sb.append(integerStringTuple2+",");
                    i++;
                }
                sb.append("]");
                sb.append('\n');
            }

            bufferedWriter.write(sb.toString());
            bufferedWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeQuery1(List<Tuple2<Tuple2<String,Integer>,  Integer>> list) {

        //importo files di output su hdfs
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hdfs-namenode:9000");
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(configuration);
            Path hdfsWritePath = new Path("hdfs://hdfs-namenode:9000"+filePath_resQuery1);
            FSDataOutputStream fsDataOutputStream = null;
            fsDataOutputStream = hdfs.create(hdfsWritePath,true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));

            //scrittura su csv
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

            bufferedWriter.write(sb.toString());
            bufferedWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
