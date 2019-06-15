package com.mapohl.gtfsdatapipeline.utils;

import com.mapohl.gtfsdatapipeline.domain.GtfsArrival;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.function.Predicate;

@AllArgsConstructor
@Data
public class GeoPredicate implements Predicate<GtfsArrival> {

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
    public boolean test(GtfsArrival gtfsArrival) {
        return radiusInMetres >= distToCenter(gtfsArrival.getLat(), gtfsArrival.getLon());
    }
}
