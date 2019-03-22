package com.mapohl.gtfsdatapipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class GtfsArrival implements Comparable<GtfsArrival> {

    private final LocalDateTime localTime;

    private final String stopName;

    private final double lat;
    private final double lon;

    public GtfsArrival(LocalDateTime localTime, String stopName, double lat, double lon) {
        this.localTime = localTime;
        this.stopName = stopName;
        this.lat = lat;
        this.lon = lon;
    }

    @JsonIgnore
    public LocalDateTime getLocalTime() {
        return this.localTime;
    }

    @JsonProperty("local-time")
    public String getLocalTimeStr() {
        return this.localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS"));
    }

    @JsonProperty("name")
    public String getStopName() {
        return this.stopName;
    }

    @JsonProperty("latitude")
    public double getLatitude() {
        return this.lat;
    }

    @JsonProperty("longitude")
    public double getLongitude() {
        return this.lon;
    }

    @Override
    public int compareTo(GtfsArrival o) {
        return this.getLocalTime().compareTo(o.getLocalTime());
    }
}
