package com.mapohl.gtfsdatapipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.*;

public class GtfsArrivalsProducer implements Callable<Void>, AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(GtfsArrivalsProducer.class);

    private class GtfsArrivalSelector implements Runnable {

        private LocalDateTime startTime;
        private Duration duration;

        public GtfsArrivalSelector(LocalDateTime startTime,
                                   int numberOfDays) {
            this.startTime = startTime;
            this.duration = Duration.ofDays(numberOfDays);
        }

        @Override
        public void run() {
            LocalDateTime sTime = this.startTime;

            // update startTime for next run
            this.startTime = sTime.plus(this.duration);
            try {
                GtfsArrivalsProducer.this.dao.getGtfsArrivals(GtfsArrivalsProducer.this.arrivalQueue, sTime, this.duration);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private class ShutdownHook extends Thread {

        @Override
        public void run() {
            super.run();

            try {
                GtfsArrivalsProducer.this.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                logger.info("All components have been shutdown.");
            }
        }
    }

    private GtfsDAO dao;

    // GtfsArrival implements Comparable based on its time
    private Queue<GtfsArrival> arrivalQueue = Queues.newPriorityBlockingQueue();

    private ScheduledExecutorService threadPool = Executors.newSingleThreadScheduledExecutor();

    private KafkaProducer<Long, String> producer;

    private LocalDateTime startTime;
    private long timeDiff = -1;

    // ***********************************
    // command line parameters
    @CommandLine.Option(names={"-t", "--topic"}, required = true)
    private String topic;

    @CommandLine.Option(names={"--dbhost", "-h"})
    private String databaseHost = "localhost";
    @CommandLine.Option(names={"--dbport", "-p"})
    private int databasePort = 5432;
    @CommandLine.Option(names={"--dbname", "-n"})
    private String databaseName = "gtfs";
    @CommandLine.Option(names={"--dbuser", "-user"})
    private String databaseUser = "gtfs";
    @CommandLine.Option(names={"--dbpassword", "-pw"})
    private String databasePassword = "gtfs";

    @CommandLine.Option(names={"--kafka-server", "-k"}, required = true)
    private String kafkaServersString;
    @CommandLine.Option(names={"--kafka-client", "-c"}, required = true)
    private String kafkaClientId = "gtfs-arrivals-producer-";

    @CommandLine.Option(names={"--start", "-s"}, required = true)
    private String startTimeStr;
    @CommandLine.Option(names={"--days", "-d"})
    private int pollIntervalInDays = 7;
    @CommandLine.Option(names={"--initial-delay", "-i"})
    private int initialDelay = 0;

    private void init() throws SQLException {
        // initialize database access
        this.dao = new GtfsDAO(this.databaseHost, this.databasePort, this.databaseName, this.databaseUser, this.databasePassword);

        // transform date string into LocalDateTime instance
        this.startTime = LocalDateTime.parse(this.startTimeStr, DateTimeFormatter.ISO_DATE_TIME);

        // initialize thread for selecting the GtfsArrival instances
        this.threadPool.scheduleAtFixedRate(
                new GtfsArrivalSelector(this.startTime, this.pollIntervalInDays),
                this.initialDelay,
                this.pollIntervalInDays, TimeUnit.DAYS);

        // initialize Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaServersString);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, this.kafkaClientId + System.currentTimeMillis());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);

        // add shutdown hook for closing all connections in case of external shutdown
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }

    public Void call() throws ExecutionException, InterruptedException, JsonProcessingException, SQLException {
        this.init();

        int pollSleepTime = 1000;
        try {
            ObjectMapper objMapper = new ObjectMapper();
            while (true) {
                GtfsArrival arrival = this.arrivalQueue.poll();

                if (arrival == null) {
                    // wait if no arrival can be processed
                    logger.info("Wait for {} millisecond.", pollSleepTime);
                    Thread.sleep(pollSleepTime);
                    pollSleepTime = pollSleepTime > 60000 ? 1000 : pollSleepTime * 2;
                    continue;
                } else {
                    // reinitialize the queue poll wait time for the next poll
                    pollSleepTime = 1000;
                }

                // determine the difference in time between the first arrival and the current time
                LocalDateTime now = LocalDateTime.now();
                if (this.timeDiff < 0) {
                    this.timeDiff = arrival.getLocalTime().until(now, ChronoUnit.MILLIS);
                }

                // wait to simulate the time difference between the last arrival and the current one
                long waitTime = Math.max(0, arrival.getLocalTime().until(now, ChronoUnit.MILLIS) - this.timeDiff);

                logger.debug("Arrival processed. {}ms waiting time (current time: {}, arrival time: {}, set difference: {}ms)",
                        waitTime,
                        now.toString(),
                        arrival.getLocalTime().toString(),
                        this.timeDiff);

                Thread.sleep(waitTime);

                // create record and send it
                final ProducerRecord<Long, String> record = new ProducerRecord<>(this.topic, System.currentTimeMillis(), objMapper.writeValueAsString(arrival));

                RecordMetadata metadata = this.producer.send(record).get();
                logger.info("Sent record(key={} stop-name={}) meta(partition={}, offset={})",
                        record.key(), arrival.getStopName(), metadata.partition(),
                        metadata.offset());
            }
        } finally {
            this.close();
        }
    }

    @Override
    public void close() throws SQLException {
        this.dao.close();

        this.producer.flush();
        this.producer.close();
    }

    public static void main(String[] args) {
        CommandLine.call(new GtfsArrivalsProducer(), args);
    }
}
