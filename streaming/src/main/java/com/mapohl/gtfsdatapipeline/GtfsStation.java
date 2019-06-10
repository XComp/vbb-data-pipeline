package com.mapohl.gtfsdatapipeline;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

import java.time.LocalDateTime;

@AllArgsConstructor
@Data
public class GtfsStation {

    @NonNull
    private final String stopName;

    private final double latitude;
    private final double longitude;

    public GtfsArrival createArrival(LocalDateTime arrivalTime) {
        return new GtfsArrival(arrivalTime, this.getStopName(), this.getLatitude(), this.getLongitude());
    }
}
