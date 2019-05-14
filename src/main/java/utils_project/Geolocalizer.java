package utils_project;

import com.mapbox.api.geocoding.v5.GeocodingCriteria;
import com.mapbox.api.geocoding.v5.MapboxGeocoding;
import com.mapbox.api.geocoding.v5.models.GeocodingResponse;
import com.mapbox.geojson.Point;
import retrofit2.Response;

import java.io.IOException;

public class Geolocalizer {

    private static String MAPBOX_ACCESS_TOKEN = "pk.eyJ1IjoicmFuZm8iLCJhIjoiY2p2Y2VqdmU0MXd4YTQzbnQ2bmR2YXBsbCJ9.3PrF9FGN4L1OAx5ajfDddw";

    public String localize(String latitude, String longitude) throws IOException {

        MapboxGeocoding reverseGeocode = MapboxGeocoding.builder()
                .accessToken(MAPBOX_ACCESS_TOKEN)
                .query(Point.fromLngLat(Double.parseDouble(longitude),Double.parseDouble(latitude)))
                .geocodingTypes(GeocodingCriteria.TYPE_COUNTRY)
                .build();


        Response<GeocodingResponse> response = reverseGeocode.executeCall();
        return response.body().features().get(0).placeName();

    }


}
