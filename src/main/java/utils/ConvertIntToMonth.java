package utils;

import java.util.Arrays;
import java.util.List;

public class ConvertIntToMonth {


    public static String convert(int n){

        List<String> months = Arrays.asList("Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno", "Luglio", "Agosto",
                "Settembre", "Ottobre", "Novembre", "Dicembre");

        return months.get(n);


    }


}
