package com.mapohl.gtfsdatapipeline.data;

import com.google.common.collect.Sets;
import com.mapohl.gtfsdatapipeline.domain.GtfsArrival;
import com.mapohl.gtfsdatapipeline.domain.GtfsStation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Queue;
import java.util.Set;

public class GtfsDAO implements AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(GtfsDAO.class);

    private Connection connection;
    private PreparedStatement preparedStatement;

    public GtfsDAO(String host,
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
                "SELECT monday, tuesday, wednesday, thursday, friday, saturday, sunday, " +
                        "stop_name, " +
                        "arrival_time, " +
                        "stop_lat, " +
                        "stop_lon, " +
                        "start_date, " +
                        "end_date " +
                        "FROM calendar JOIN trips ON (calendar.run_id, calendar.service_id) = (trips.run_id, trips.service_id) " +
                        "   JOIN stop_times ON (trips.run_id, trips.trip_id) = (stop_times.run_id, stop_times.trip_id) " +
                        "   JOIN stops ON (stop_times.run_id, stop_times.stop_id) = (stops.run_id, stops.stop_id) " +
                        "WHERE end_date >= ? AND start_date < ? " +
                        "ORDER BY arrival_time;");
    }

    private static int extractDateInt(LocalDateTime d) {
        return d.getYear() * 10000 + d.getMonthValue() * 100 + d.getDayOfMonth();
    }

    private static LocalDateTime createLocalDateTime(int date, Time time) {
        int year = date / 10000;
        int month = (date - year * 10000) / 100;
        int day = date - year * 10000 - month * 100;

        LocalTime localTime = time.toLocalTime();

        return LocalDateTime.of(year, month, day, localTime.getHour(), localTime.getMinute(), localTime.getSecond());
    }

    private Set<Integer>[] detectWeekDays(LocalDateTime start, int startInt, int endInt) {
        Set<Integer>[] weekDays = new Set[7];

        // initialize array collecting the date ints per weekday
        for (int i = 0; i < weekDays.length; i++) {
            weekDays[i] = Sets.newHashSet();
        }

        // initialize the for loop by detecting what kind of weekday the start date is
        int startWeekDay = start.getDayOfWeek().getValue() - 1;
        for (int day = startInt, weekDay = startWeekDay; day < endInt; day++, weekDay++) {
            weekDay %= weekDays.length;
            weekDays[weekDay].add(day);
        }

        return weekDays;
    }

    public void getGtfsArrivals(Queue<GtfsArrival> arrivalQueue, LocalDateTime start, Duration duration) throws SQLException {
        int startInt = extractDateInt(start);
        int endInt = extractDateInt(start.plus(duration));

        this.preparedStatement.clearParameters();
        this.preparedStatement.setInt(1, startInt);
        this.preparedStatement.setInt(2, endInt);

        logger.debug("Initialized SQL parameters: 1={}, 2={}", startInt, endInt);

        // collect weekdays within the given time frame
        Set<Integer>[] weekDays = detectWeekDays(start, startInt, endInt);

        ResultSet result = this.preparedStatement.executeQuery();
        logger.debug("SQL query processed");
        while (result.next()) {
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
                        GtfsArrival arrival = station.createArrival(createLocalDateTime(dayInt, result.getTime("arrival_time")));
                        arrivalQueue.add(arrival);
                    }
                }
            }
        }
    }


    @Override
    public void close() throws SQLException {
        this.preparedStatement.close();
        this.connection.close();
    }
}
