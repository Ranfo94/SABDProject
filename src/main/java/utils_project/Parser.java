package utils_project;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class Parser {

    /**
     * Funzione con il compito di leggere il file contenente città e coordinate, in modo tale da ricavare
     * la lista delle città
     */
    public static ArrayList<String> findCities(String pathToHumidityFile) {

        int i;
        String cities = "";

        try {

            BufferedReader reader = new BufferedReader(new FileReader(pathToHumidityFile));
            cities = reader.readLine();

        } catch (IOException e) {
            e.printStackTrace();
        }

        String[] cities2 = cities.split(",");
        ArrayList<String> city = new ArrayList<>();

        for(i=1; i<cities2.length; ++i)
            city.add(cities2[i]);
        return city;
    }


    /**
     * Filtra il file passato come argomento, togliendo tutte le righe incomplete. Salta la prima riga perchè composta
     * dalle città. Tale riga viene analizzata separatamente
     */
    public static ArrayList<String> filtraFile(String pathToFile) {

        String line;
        ArrayList<String> file = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(pathToFile));

            //utilizzo la 1^ riga per determinare la lunghezza delle righe complete
            line = reader.readLine();
            String[] words = line.split(",");
            int lineDimension = words.length;
            //System.out.println(lineDimension + " " + line);
            while ((line = reader.readLine()) != null) {

                boolean trovato = line.contains(",,");  //evito di prendere linee con valori mancanti

                if (!trovato) {
                    words = line.split(",");

                    if(words.length == lineDimension) {
                        file.add(line);
                    }
                    /*
                    else {
                        System.out.println(pathToFile);
                        System.out.println("lunghezza diversa: " + line);
                        //exit(0);
                    }
                    */
                }
            }
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return file;
    }


    /**
     * Cerca le singole nazioni presenti nella lista di città
     * @param countries
     * @return
     */
    public static ArrayList<String> findDistinctCountries(ArrayList<String> countries){
        ArrayList<String> result = new ArrayList<>();

        int i;
        for(i=0; i<countries.size(); ++i){
            if(!result.contains(countries.get(i)))
                result.add(countries.get(i));
        }
        return result;
    }


    public static Integer findCountryIndex(String country, ArrayList<String> distinctCountries) {
        boolean trovato = false;
        int i;
        Integer index = null;
        for(i=0; i<distinctCountries.size() && trovato==false; ++i){
            if(distinctCountries.get(i).equals(country)) {
                trovato = true;
                index = i;
            }
        }
        return index;
    }
}
