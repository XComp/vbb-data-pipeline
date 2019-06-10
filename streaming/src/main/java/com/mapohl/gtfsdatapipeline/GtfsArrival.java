package com.mapohl.gtfsdatapipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@AllArgsConstructor
@Data
@C
public class GtfsArrival implements Comparable<GtfsArrival> {

    @NonNull
    private final LocalDateTime localTime;

    @NonNull
    @JsonProperty("name")
    private final String stopName;

    @JsonProperty("latitude")
    private final double lat;
    @JsonProperty("longitude")
    private final double lon;

    @JsonIgnore
    public LocalDateTime getLocalTime() {
        return this.localTime;
    }

    @JsonProperty("local-time")
    public String getLocalTimeStr() {
        return this.localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS"));
    }

    @Override
    public int compareTo(GtfsArrival o) {
        return this.getLocalTime().compareTo(o.getLocalTime());
    }
}
