package com.mapohl.gtfsdatapipeline;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Set;

public class GtfsStopTimesSelector {

    private Connection connection;
    private PreparedStatement preparedStatement;

    public GtfsStopTimesSelector(String host,
                                 int port,
                                 String dbname,
                                 String user,
                                 String password) throws SQLException {
        this.connection = DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%d/%s",
                        host,
                        port,
                        dbname),
                user,
                password);

        this.preparedStatement =  this.connection.prepareStatement(
                "SELECT monday, tuesday, wednesday, thursday, friday, saturday, sunday, stop_name, arrival_time, stop_lat, stop_lon, start_date, end_date " +
                        "FROM calendar JOIN trips ON (calendar.run_id, calendar.service_id) = (trips.run_id, trips.service_id) " +
                        "   JOIN stop_times ON (trips.run_id, trips.trip_id) = (stop_times.run_id, stop_times.trip_id) " +
                        "   JOIN stops ON (stop_times.run_id, stop_times.stop_id) = (stops.run_id, stops.stop_id) " +
                        "WHERE end_date >= ? AND start_date < ?;");
    }

    private static int extractDateInt(LocalDateTime d) {
        return d.getYear() * 10000 + d.getMonthValue() * 100 + d.getDayOfMonth();
    }

    private static LocalDateTime createLocalDateTime(int date, Time time) {
        int year = date / 10000;
        int month = date / 100 - year;
        int day = date - year - month;

        LocalTime localTime = time.toLocalTime();

        return LocalDateTime.of(year, month, day, localTime.getHour(), localTime.getMinute(), localTime.getSecond());
    }

    private Set<Integer>[] detectWeekDays(LocalDateTime start, int startInt, int endInt) {
        Set<Integer>[] weekDays = new Set[7];
        for (int i = 0; i < weekDays.length; i++) {
            weekDays[i] = Sets.newHashSet();
        }
        int startWeekDay = start.getDayOfWeek().getValue() - 1;
        for (int day = startInt, weekDay = startWeekDay; day < endInt; day++, weekDay++) {
            weekDay %= weekDays.length;
            weekDays[weekDay].add(day);
        }

        return weekDays;
    }

    public Collection<GtfsArrival> getGtfsArrivals(LocalDateTime start, Duration duration) throws SQLException {
        int startInt = extractDateInt(start);
        int endInt = extractDateInt(start.plus(duration));

        this.preparedStatement.clearParameters();
        for (int i = 1; i < 6; i+=2) {
            this.preparedStatement.setInt(i, startInt);
            this.preparedStatement.setInt(i + 1, endInt);
        }

        Set<Integer>[] weekDays = detectWeekDays(start, startInt, endInt);

        Collection<GtfsArrival> arrivals = Lists.newArrayList();
        ResultSet result = this.preparedStatement.executeQuery();
        while (!result.isAfterLast()) {
            GtfsStation station = new GtfsStation(
                    result.getString("stop_name"),
                    result.getDouble("stop_lat"),
                    result.getDouble("stop_lon")
            );

            int recordStartInt = result.getInt("start_date");
            int recordEndInt = result.getInt("end_date");

            for (int i = 0; i < weekDays.length; i++) {
                if (!result.getBoolean(i + 1)) {
                    // time is not valid for this weekday
                    continue;
                }

                for (Integer dayInt : weekDays[i]) {
                    if (dayInt >= recordStartInt || dayInt < recordEndInt) {
                        // only add the arrival if it is within the validity timeframe of this record
                        arrivals.add(station.createArrival(createLocalDateTime(dayInt, result.getTime("arrival_time"))));
                    }
                }
            }
            result.next();
        }

        return arrivals;
    }


}
