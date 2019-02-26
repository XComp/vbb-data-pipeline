#!/bin/bash

set -e
set -u

PGPASSWORD="$GTFS_DB_PASSWORD" psql -v ON_ERROR_STOP=1 -d "$GTFS_DB" -U "$GTFS_DB_USER" <<-EOSQL
    CREATE SCHEMA gtfs;

    CREATE TABLE gtfs.provider (
        provider_id             VARCHAR(8) PRIMARY KEY NOT NULL,
        created                 TIMESTAMP NOT NULL,
        feed_publisher_name     TEXT NOT NULL,
        feed_publisher_url      TEXT NOT NULL,
        feed_lang               TEXT NOT NULL
    );

    CREATE TABLE gtfs.run (
        run_id                  INT PRIMARY KEY NOT NULL,
        run_date                VARCHAR(10) NOT NULL,
        provider_id             VARCHAR(8) NOT NULL REFERENCES gtfs.provider(provider_id)
    );

    CREATE TABLE gtfs.agency (
        run_id                  INT NOT NULL REFERENCES gtfs.run(run_id),
        agency_id               TEXT PRIMARY KEY NOT NULL,
        agency_name             TEXT NOT NULL,
        agency_url              TEXT NOT NULL,
        agency_timezone         TEXT NOT NULL,
        agency_lang             TEXT NULL,
        agency_phone            TEXT NULL
    );

    CREATE TABLE gtfs.stops (
        run_id                  INT NOT NULL REFERENCES gtfs.run(run_id),
        stop_id                 TEXT PRIMARY KEY,
        stop_code               TEXT NULL,
        stop_name               TEXT NOT NULL,
        stop_desc               TEXT NULL,
        stop_lat                DOUBLE PRECISION NOT NULL,
        stop_lon                DOUBLE PRECISION NOT NULL,
        zone_id                 TEXT NULL,
        stop_url                TEXT NULL,
        location_type           BOOLEAN NULL,
        parent_station          TEXT NULL
    );

    CREATE TABLE gtfs.routes (
        run_id                  INT NOT NULL REFERENCES gtfs.run(run_id),
        route_id                TEXT PRIMARY KEY,
        agency_id               TEXT NULL,
        route_short_name        TEXT NULL,
        route_long_name         TEXT NULL,
        route_desc              TEXT NULL,
        route_type              INT NULL,
        route_url               TEXT NULL,
        route_color             TEXT NULL,
        route_text_color        TEXT NULL
    );

    CREATE TABLE gtfs.calendar (
        run_id                  INT NOT NULL REFERENCES gtfs.run(run_id),
        service_id              TEXT PRIMARY KEY,
        monday                  BOOLEAN NOT NULL,
        tuesday                 BOOLEAN NOT NULL,
        wednesday               BOOLEAN NOT NULL,
        thursday                BOOLEAN NOT NULL,
        friday                  BOOLEAN NOT NULL,
        saturday                BOOLEAN NOT NULL,
        sunday                  BOOLEAN NOT NULL,
        start_date              NUMERIC(8) NOT NULL,
        end_date                NUMERIC(8) NOT NULL
    );

    CREATE TABLE gtfs.calendar_dates (
        run_id                  INT NOT NULL REFERENCES gtfs.run(run_id),
        service_id              TEXT NOT NULL,
        date                    NUMERIC(8) NOT NULL,
        exception_type          INT NOT NULL
    );

    CREATE TABLE gtfs.shapes (
        run_id                  INT NOT NULL REFERENCES gtfs.run(run_id),
        shape_id                TEXT,
        shape_pt_lat            DOUBLE PRECISION NOT NULL,
        shape_pt_lon            DOUBLE PRECISION NOT NULL,
        shape_pt_sequence       INT NOT NULL,
        shape_dist_traveled     TEXT NULL
    );

    CREATE TABLE gtfs.trips (
        run_id                  INT NOT NULL REFERENCES gtfs.run(run_id),
        route_id                TEXT NOT NULL,
        service_id              TEXT NOT NULL,
        trip_id                 TEXT NOT NULL PRIMARY KEY,
        trip_headsign           TEXT NULL,
        direction_id            BOOLEAN NULL,
        block_id                TEXT NULL,
        shape_id                TEXT NULL
    );

    CREATE TABLE gtfs.stop_times (
        run_id                  INT NOT NULL REFERENCES gtfs.run(run_id),
        trip_id                 TEXT NOT NULL,
        arrival_time            INTERVAL NOT NULL,
        departure_time          INTERVAL NOT NULL,
        stop_id                 TEXT NOT NULL,
        stop_sequence           INT NOT NULL,
        stop_headsign           TEXT NULL,
        pickup_type             INT NULL CHECK(pickup_type >= 0 and pickup_type <=3),
        drop_off_type           INT NULL CHECK(drop_off_type >= 0 and drop_off_type <=3),
        shape_dist_traveled     INT NULL
    );

    CREATE TABLE gtfs.transfers (
        run_id                  INT NOT NULL REFERENCES gtfs.run(run_id),
        from_stop_id            TEXT NOT NULL,
        to_stop_id              TEXT NOT NULL,
        transfer_type           INT NOT NULL,
        min_transfer_time       INT
    );
EOSQL