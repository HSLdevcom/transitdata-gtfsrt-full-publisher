package fi.hsl.transitdata.gtfsbundler;

import com.typesafe.config.Config;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageProcessor implements IMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private Consumer<byte[]> consumer;
    final ScheduledExecutorService scheduler;

    final List<DatasetEntry> inputQueue = new LinkedList<>();
    final DatasetBundler bundler;

    private MessageProcessor(final PulsarApplication app, DatasetBundler bundler) {

        this.consumer = app.getContext().getConsumer();
        Config config = app.getContext().getConfig();
        this.bundler = bundler;

        long intervalInSecs = config.getInt("bundler.dumpIntervalInSecs");
        log.info("Dump interval {} seconds", intervalInSecs);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        log.info("Starting result-scheduler");

        scheduler.scheduleAtFixedRate(() -> {
            try {
                log.debug("Checking results!");
                dump();
            }
            catch (Exception e) {
                log.error("Failed to check results, closing application", e);
                closeApplication(app, scheduler);
            }
        }, intervalInSecs, intervalInSecs, TimeUnit.SECONDS);

    }

    public static MessageProcessor newInstance(final PulsarApplication app) throws Exception {
        DatasetBundler bundler = DatasetBundler.newInstance(app.getContext().getConfig());
        return new MessageProcessor(app, bundler);
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        scheduler.shutdown();
        app.close();
    }

    private void dump() throws Exception {
        List<DatasetEntry> copy = new LinkedList<>();
        synchronized (inputQueue) {
            copy.addAll(inputQueue);
            inputQueue.clear();
        }

        log.info("Dump-time, new messages: {}", copy.size());
        try {
            bundler.bundle(copy);
        }
        catch (Exception e) {
            log.error("Failed to bundle Full GTFS-RT dataset", e);
            throw e;
        }
    }

    @Override
    public void handleMessage(final Message msg) throws Exception {
        try {
            parseProtobufSchema(msg).ifPresent(schema -> {
                try {
                    if (schema == TransitdataProperties.ProtobufSchema.GTFS_TripUpdate ||
                        schema == TransitdataProperties.ProtobufSchema.GTFS_ServiceAlert) {
                        handleFeedMessage(msg);
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

    private void handleFeedMessage(final Message msg) throws Exception {
        DatasetEntry entry = DatasetEntry.newEntry(msg);
        synchronized (inputQueue) {
            inputQueue.add(entry);
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
            log.error("Failed to parse protobuf schema", e);
            return Optional.empty();
        }
    }

    public void close() {
        log.info("Closing poller");
        scheduler.shutdown();
    }

}
