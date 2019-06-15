package com.mapohl.gtfsdatapipeline.utils

import com.mapohl.gtfsdatapipeline.domain.GtfsArrival
import spock.lang.Specification
import spock.lang.Unroll

import java.time.LocalDateTime

class GeoPredicateSpec extends Specification {

    @Unroll
    def "test radius filter"() {
        given:
        GtfsArrival arrival = new GtfsArrival(
                LocalDateTime.now(),
                "random-stop",
                lat,
                lon
        )
        when:
        GeoPredicate pred = new GeoPredicate(centerLat, centerLon, radiusInMetres)

        then:
        assert pred.test(arrival) == isCloseEnough

        where:
        centerLat | centerLon | radiusInMetres | lat      | lon      | isCloseEnough
        0.0       | 0.0       | 0              | 0.0      | 0.0      | true
        0.0       | 0.0       | 1_000_000      | 1.0      | 0.0      | true
        0.0       | 0.0       | 1_000          | 1.0      | 0.0      | false
        52.52437  | 13.41053  | 30_000         | 52.39886 | 13.06566 | true
        52.52437  | 13.41053  | 25_000         | 52.39886 | 13.06566 | false
    }
}
