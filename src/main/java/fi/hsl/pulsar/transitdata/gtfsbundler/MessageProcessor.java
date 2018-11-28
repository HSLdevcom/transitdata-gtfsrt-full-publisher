package fi.hsl.pulsar.transitdata.gtfsbundler;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MessageProcessor implements IMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private Consumer<byte[]> consumer;
    final ScheduledExecutorService scheduler;

    final Queue<GtfsRealtime.TripUpdate> inputQueue = new LinkedList<>();

    public MessageProcessor(PulsarApplicationContext context) {
        this.consumer = context.getConsumer();
        Config config = context.getConfig();
        long intervalInSecs = config.getInt("bundler.dumpIntervalInSecs");
        log.info("Dump interval {} seconds", intervalInSecs);

        scheduler = Executors.newSingleThreadScheduledExecutor();
        log.info("Starting result-scheduler");

        scheduler.scheduleAtFixedRate(() -> {
            try {
                log.debug("Checking results!");
                dump();
                //clean(); //Possibly also clean old files?
            }
            catch (Exception e) {
                log.error("Failed to check results", e);
            }

        }, intervalInSecs, intervalInSecs, TimeUnit.SECONDS);

    }

    private void dump() {
        Queue<GtfsRealtime.TripUpdate> copy = new LinkedList<>();
        synchronized (inputQueue) {
            copy.addAll(inputQueue);
            inputQueue.clear();
        }
        //TODO magic
        log.info("Dump-time, new messages: {}", copy.size());
    }

    @Override
    public void handleMessage(final Message msg) throws Exception {
        try {
            parseProtobufSchema(msg).ifPresent(schema -> {
                try {
                    if (schema == TransitdataProperties.ProtobufSchema.GTFS_TripUpdate) {
                        handleTripUpdateMessage(msg);
                    }
                    else {
                        log.info("Ignoring message of schema " + schema);
                    }
                }
                catch (Exception e) {
                    log.error("Failed to handle message for schema " + schema, e);
                }
            });

            //Ack Pulsar message
            consumer.acknowledgeAsync(msg).thenRun(() -> {
                log.debug("Message acked");
            });
        }
        catch (Exception e) {
            log.error("Unknown error, existing app", e);
            close();
            throw e;
        }
    }

    private void handleTripUpdateMessage(final Message msg) throws Exception {
        GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(msg.getData());

        List<GtfsRealtime.TripUpdate> tripUpdates = feedMessage.getEntityList()
                .stream()
                .flatMap(
                        entity -> entity.hasTripUpdate() ? Stream.of(entity.getTripUpdate()) : Stream.empty()
                ).collect(Collectors.toList());

        synchronized (inputQueue) {
            inputQueue.addAll(tripUpdates);
        }
    }


    private Optional<TransitdataProperties.ProtobufSchema> parseProtobufSchema(Message received) {
        try {
            String schemaType = received.getProperty(TransitdataProperties.KEY_PROTOBUF_SCHEMA);
            log.debug("Received message with schema type " + schemaType);
            TransitdataProperties.ProtobufSchema schema = TransitdataProperties.ProtobufSchema.fromString(schemaType);
            return Optional.of(schema);
        }
        catch (Exception e) {
            //log.error("Failed to parse protobuf schema", e);
            //return Optional.empty();
            //DEBUG, now the TripUpdateProcessor doesn't yet output this. TODO fix once PR merged
            return Optional.of(TransitdataProperties.ProtobufSchema.GTFS_TripUpdate);
        }
    }

    public void close() {
        log.info("Closing poller");
        scheduler.shutdown();
    }

}
