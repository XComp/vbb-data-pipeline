package com.mapohl.gtfsdatapipeline;


import java.time.LocalDateTime;

public class GtfsStation {

    private final String stopName;

    private final double lat;
    private final double lon;

    public GtfsStation(String stopName, double lat, double lon) {
        this.stopName = stopName;
        this.lat = lat;
        this.lon = lon;
    }

    public GtfsArrival createArrival(LocalDateTime arrivalTime) {
        return new GtfsArrival(arrivalTime, this.getStopName(), this.getLatitude(), this.getLongitude());
    }

    public String getStopName() {
        return this.stopName;
    }

    public double getLatitude() {
        return this.lat;
    }

    public double getLongitude() {
        return this.lon;
    }
}
