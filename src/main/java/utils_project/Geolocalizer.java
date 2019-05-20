package utils_project;

import com.mapbox.api.geocoding.v5.GeocodingCriteria;
import com.mapbox.api.geocoding.v5.MapboxGeocoding;
import com.mapbox.api.geocoding.v5.models.GeocodingResponse;
import com.mapbox.geojson.Point;
import entities.City;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import retrofit2.Response;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Geolocalizer {

    private static String MAPBOX_ACCESS_TOKEN = "pk.eyJ1IjoicmFuZm8iLCJhIjoiY2p2Y2VqdmU0MXd4YTQzbnQ2bmR2YXBsbCJ9.3PrF9FGN4L1OAx5ajfDddw";

    /*
     *
     * STARTING FROM CITIES COORDS CREATE PAIR RDD WITH (CITY NAME,CITY OBJECT)
     *
     * */
    public static HashMap<String, City> process_city_location(JavaRDD<String> cityData) throws IOException {
        //todo: arraylist of cities;

        HashMap<String,City> countrymap = new HashMap<>();
        String firstRow = cityData.first();
        JavaRDD<String> withoutfirst = cityData.filter(x -> !x.equals(firstRow));

        for (String line : withoutfirst.collect()){
            String[] splittedLine = line.split(",");
            long lat = (long) Double.parseDouble(splittedLine[1]);
            long lon = (long) Double.parseDouble(splittedLine[2]);

            String country = new Geolocalizer().localize(splittedLine[1],splittedLine[2]);
            int offset = TimeDateManager.getTimeZoneOffset(lat,lon);
            City city = new City(splittedLine[0],country,lat,lon,offset);
            countrymap.put(splittedLine[0],city);
        }
        return countrymap;
    }


    public String localize(String latitude, String longitude) throws IOException {

        MapboxGeocoding reverseGeocode = MapboxGeocoding.builder()
                .accessToken(MAPBOX_ACCESS_TOKEN)
                .query(Point.fromLngLat(Double.parseDouble(longitude),Double.parseDouble(latitude)))
                .geocodingTypes(GeocodingCriteria.TYPE_COUNTRY)
                .build();


        Response<GeocodingResponse> response = reverseGeocode.executeCall();
        return response.body().features().get(0).placeName();

    }


    public ArrayList<String> findCountries(String pathToCityFile){

        String line;
        ArrayList<String> countries = new ArrayList<>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(pathToCityFile));
            line = reader.readLine();
            while(true){
                line = reader.readLine();
                if(line==null)
                    break;
                String[] word = line.split(",");

                String country = new Geolocalizer().localize(word[1], word[2]);
                countries.add(country);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return countries;
    }
}
