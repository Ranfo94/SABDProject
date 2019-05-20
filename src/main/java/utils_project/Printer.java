package utils_project;

import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Printer {

    /**
     * Funzione per stampare un file
     * @param file
     * @throws IOException
     */
    public static void stampaFile(ArrayList<String> file) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(file)));
        int i=0;
        String l = "";
        while((l = reader.readLine()) != null && i<5){
            System.out.println(l);
            ++i;
        }
    }


    /**
     * Funzione per stampare una lista di stringhe
     *
     */
    public static void stampaCollezione(List<String> list){

        int i;
        for(i=0; i< list.size(); ++i)
        //for(i=0; i<5; ++i)
            System.out.println(list.get(i));
    }


    /**
     * Funzione per stampare tupla di Stringhe
     */
    public static void stampaCollezione2(List<Tuple2<String, String>> list) {

        int i;
        //for(i=0; i< list.size(); ++i)
        for(i=0; i<50; ++i)
            System.out.println(list.get(i));
    }


    public static void stampaCollezione3(List<Tuple2<String, Integer>> list) {

        int i;
        for(i=0; i< list.size(); ++i)
            //for(i=0; i<50; ++i)
            System.out.println(list.get(i));
    }

    public static void stampaCollezione4(List<Tuple2<Integer, String>> list) {

        int i;
        for(i=0; i< list.size(); ++i)
            //for(i=0; i<50; ++i)
            System.out.println(list.get(i));
    }


    public static void stampaCollezione5(List<Tuple2<String, Iterable<String>>> list) {

        int i;
        for(i=0; i< list.size(); ++i)
            //for(i=0; i<50; ++i)
            System.out.println(list.get(i));
    }


    public static void stampaCollezione6(List<Tuple2<String, Iterable<String>>> list) {
        int i;
        for(i=0; i<list.size(); ++i)
            System.out.println(list.get(i));
        System.out.println("");
    }


    public static void stampaListaGiornaliera(List<Tuple2<String, Double>> list) {
        int i;
        for(i=0; i<list.size(); ++i) {
            if(list.get(i)._1.contains("2016-05-13") && list.get(i)._1.contains("Beersheba"))
                System.out.println(list.get(i));
        }
        System.out.println("");
    }

    public static void stampaListaMensile(List<Tuple2<String, Double>> list) {
        int i;
        for(i=0; i<list.size(); ++i) {
            if(list.get(i)._1.contains("2016-05") && list.get(i)._1.contains("Beersheba"))
                System.out.println(list.get(i));
        }
        System.out.println("");
    }

    public static void stampaListaAnnuale(List<Tuple2<String, Double>> list) {
        int i;
        for(i=0; i<list.size(); ++i) {
            if(list.get(i)._1.contains("2016") && list.get(i)._1.contains("Beersheba"))
                System.out.println(list.get(i));
        }
        System.out.println("");
    }

    public static void stampaCollezioneAnnuale(List<Tuple2<String, Double>> list) {
        int i;
        for(i=0; i<list.size(); ++i) {
            if(list.get(i)._1.contains("2016 Beersheba")) {

                System.out.println(list.get(i)._1 + ",   " + "value = " + list.get(i)._2);
            }
        }
        System.out.println("");
    }

    public static void stampaDifferenzaQuadrimestri(List<Tuple2<String, Double>> list) {
        int i;
        for (i = 0; i < list.size(); ++i) {
            if(list.get(i)._1.contains("2016 Beersheba")) {

                Double value = list.get(i)._2;
                if (value < 0)
                    value = -value;
                System.out.println(list.get(i)._1 + ",   " + "value = " + value);
            }
        }
        System.out.println("");
    }

    public static void stampaRisultato(List<Tuple2<Double, String>> list) {
        int i;
        for(i=0; i< list.size(); ++i) {
            //if (list.get(i)._2().contains("Los Angeles") || list.get(i)._2().contains("Beersheba"))
                System.out.println(list.get(i));
        }
    }


    public static void stampaListaTripla(List<Tuple3<String, String, Double>> list){
        int i;
        for(i=0; i<list.size(); ++i)
            System.out.println(list.get(i));
    }
}
