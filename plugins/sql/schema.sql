DROP TABLE IF EXISTS vbb.agency;
DROP TABLE IF EXISTS vbb.stops;
DROP TABLE IF EXISTS vbb.routes;
DROP TABLE IF EXISTS vbb.calendar;
DROP TABLE IF EXISTS vbb.calendar_dates;
DROP TABLE IF EXISTS vbb.shapes;
DROP TABLE IF EXISTS vbb.trips;
DROP TABLE IF EXISTS vbb.stop_times;
DROP TABLE IF EXISTS vbb.transfers;

DROP SCHEMA IF EXISTS vbb;
CREATE SCHEMA vbb;

CREATE TABLE vbb.agency
(
  agency_id         text PRIMARY KEY NOT NULL,
  agency_name       text NOT NULL,
  agency_url        text NOT NULL,
  agency_timezone   text NOT NULL,
  agency_lang       text NULL,
  agency_phone      text NULL
);

CREATE TABLE vbb.stops
(
  stop_id           text PRIMARY KEY,
  stop_code         text NULL,
  stop_name         text NOT NULL,
  stop_desc         text NULL,
  stop_lat          double precision NOT NULL,
  stop_lon          double precision NOT NULL,
  zone_id           text NULL,
  stop_url          text NULL,
  location_type     boolean NULL,
  parent_station    text NULL
);

CREATE TABLE vbb.routes
(
  route_id          text PRIMARY KEY,
  agency_id         text NULL,
  route_short_name  text NULL,
  route_long_name   text NULL,
  route_desc        text NULL,
  route_type        integer NULL,
  route_url         text NULL,
  route_color       text NULL,
  route_text_color  text NULL
);

CREATE TABLE vbb.calendar
(
  service_id        text PRIMARY KEY,
  monday            boolean NOT NULL,
  tuesday           boolean NOT NULL,
  wednesday         boolean NOT NULL,
  thursday          boolean NOT NULL,
  friday            boolean NOT NULL,
  saturday          boolean NOT NULL,
  sunday            boolean NOT NULL,
  start_date        numeric(8) NOT NULL,
  end_date          numeric(8) NOT NULL
);

CREATE TABLE vbb.calendar_dates
(
  service_id text NOT NULL,
  date numeric(8) NOT NULL,
  exception_type integer NOT NULL
);

CREATE TABLE vbb.shapes
(
  shape_id          text,
  shape_pt_lat      double precision NOT NULL,
  shape_pt_lon      double precision NOT NULL,
  shape_pt_sequence integer NOT NULL,
  shape_dist_traveled text NULL
);

CREATE TABLE vbb.trips
(
  route_id          text NOT NULL,
  service_id        text NOT NULL,
  trip_id           text NOT NULL PRIMARY KEY,
  trip_headsign     text NULL,
  direction_id      boolean NULL,
  block_id          text NULL,
  shape_id          text NULL
);

CREATE TABLE vbb.stop_times
(
  trip_id           text NOT NULL,
  arrival_time      interval NOT NULL,
  departure_time    interval NOT NULL,
  stop_id           text NOT NULL,
  stop_sequence     integer NOT NULL,
  stop_headsign     text NULL,
  pickup_type       integer NULL CHECK(pickup_type >= 0 and pickup_type <=3),
  drop_off_type     integer NULL CHECK(drop_off_type >= 0 and drop_off_type <=3),
  shape_dist_traveled integer NULL
);

CREATE TABLE vbb.transfers
(
    from_stop_id  text NOT NULL,
    to_stop_id    text NOT NULL,
    transfer_type   integer NOT NULL,
    min_transfer_time integer
);