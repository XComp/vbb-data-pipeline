package com.mapohl.gtfsdatapipeline.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapohl.gtfsdatapipeline.domain.GtfsArrival;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Predicate;

import java.io.IOException;

@AllArgsConstructor
@Data
@Slf4j
public class GeoPredicate implements Predicate<Long, String> {

    private static final ObjectMapper objMapper = new ObjectMapper();

    private final double centerLat;
    private final double centerLon;
    private final double radiusInMetres;

    public GeoPredicate(double lat, double lon) {
        this(lat, lon, 0.0);
    }

    // distance in metres formula was copied from StackOverflow
    // Link: https://stackoverflow.com/questions/837872/calculate-distance-in-meters-when-you-know-longitude-and-latitude-in-java
    public double distToCenter(double lat, double lon) {
        double earthRadius = 6371000; //metres
        double dLat = Math.toRadians(lat - centerLat);
        double dLng = Math.toRadians(lon - centerLon);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(Math.toRadians(centerLat)) * Math.cos(Math.toRadians(lat)) *
                        Math.sin(dLng/2) * Math.sin(dLng/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return earthRadius * c;
    }

    @Override
    public boolean test(Long key, String value) {
        GtfsArrival gtfsArrival = null;
        try {
            gtfsArrival = objMapper.readValue(value, GtfsArrival.class);
        } catch (IOException e) {
            log.error("An error occurred while parsing the JSON: " + value, e);
            return false;
        }

        return radiusInMetres >= distToCenter(gtfsArrival.getLat(), gtfsArrival.getLon());
    }
}
