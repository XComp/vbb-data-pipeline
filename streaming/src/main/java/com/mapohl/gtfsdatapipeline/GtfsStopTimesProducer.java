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

public class GtfsStopTimesProducer implements Callable<Void>, AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(GtfsStopTimesProducer.class);

    private class GtfsStopTimesSelection implements Runnable {

        private GtfsStopTimesSelector selector;
        private LocalDateTime startTime;
        private Duration duration;

        public GtfsStopTimesSelection(GtfsStopTimesSelector selector,
                                      LocalDateTime startTime,
                                      int numberOfDays) {
            this.selector = selector;
            this.startTime = startTime;
            this.duration = Duration.ofDays(numberOfDays);
        }

        @Override
        public void run() {
            LocalDateTime sTime = this.startTime;

            // update startTime for next run
            this.startTime = sTime.plus(this.duration);
            try {
                GtfsStopTimesProducer.this.arrivalQueue.addAll(this.selector.getGtfsArrivals(sTime, this.duration));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private Queue<GtfsArrival> arrivalQueue = Queues.newConcurrentLinkedQueue();

    private ScheduledExecutorService threadPool = Executors.newSingleThreadScheduledExecutor();

    private KafkaProducer<Long, String> producer;
    private long timeDiff;

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

    @CommandLine.Option(names={"--kafka", "-k"}, required = true)
    private String kafkaServersString;

    @CommandLine.Option(names={"--start", "-s"}, required = true)
    private String startTimeStr;
    @CommandLine.Option(names={"--days", "-d"})
    private int pollIntervalInDays = 7;

    private LocalDateTime startTime;

    public void init() throws SQLException {
        GtfsStopTimesSelector selector = new GtfsStopTimesSelector(
                this.databaseHost, this.databasePort, this.databaseName, this.databaseUser, this.databasePassword);

        this.startTime = LocalDateTime.parse(this.startTimeStr, DateTimeFormatter.ISO_DATE_TIME);

        this.threadPool.schedule(new GtfsStopTimesSelection(selector, this.startTime, this.pollIntervalInDays), this.pollIntervalInDays, TimeUnit.DAYS);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaServersString);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "gtfs-stop-times-producer-client-" + System.currentTimeMillis());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
        this.timeDiff = this.startTime.until(LocalDateTime.now(), ChronoUnit.MILLIS);
    }

    public Void call() throws ExecutionException, InterruptedException, JsonProcessingException, SQLException {
        this.init();

        try {
            ObjectMapper objMapper = new ObjectMapper();
            while (true) {
                GtfsArrival arrival = this.arrivalQueue.poll();

                long waitTime = Math.max(0, arrival.getLocalTime().until(LocalDateTime.now(), ChronoUnit.MILLIS) - this.timeDiff);

                Thread.sleep(waitTime);

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
    public void close() {
        this.producer.flush();
        this.producer.close();
    }

    public static void main(String[] args) throws SQLException {
        CommandLine.call(new GtfsStopTimesProducer(), args);
    }
}
