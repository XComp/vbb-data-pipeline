package com.mapohl.gtfsdatapipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class GtfsArrival {

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

    @JsonProperty
    public String getLocalTimeStr() {
        return this.localTime.format(DateTimeFormatter.RFC_1123_DATE_TIME);
    }

    @JsonProperty
    public String getStopName() {
        return this.stopName;
    }

    @JsonProperty
    public double getLatitude() {
        return this.lat;
    }

    @JsonProperty
    public double getLongitude() {
        return this.lon;
    }
}
