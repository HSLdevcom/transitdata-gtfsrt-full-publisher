package fi.hsl.transitdata.publisher;

import com.typesafe.config.Config;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
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
    final DatasetPublisher publisher;
    final DatasetPublisher.DataType dataType;

    private MessageProcessor(final PulsarApplication app, DatasetPublisher publisher) {

        this.consumer = app.getContext().getConsumer();
        Config config = app.getContext().getConfig();
        this.publisher = publisher;
        this.dataType = DatasetPublisher.DataType.valueOf(config.getString("bundler.dataType"));
        log.info("Reading data of type {} from topic {}", dataType, consumer.getTopic());

        long intervalInSecs = config.getDuration("bundler.dumpInterval", TimeUnit.SECONDS);
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
        DatasetPublisher publisher = DatasetPublisher.newInstance(app.getContext().getConfig());
        return new MessageProcessor(app, publisher);
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
            publisher.publish(copy);
        }
        catch (Exception e) {
            log.error("Failed to publish Full GTFS-RT dataset", e);
            throw e;
        }
    }

    @Override
    public void handleMessage(final Message msg) throws Exception {
        try {
            TransitdataSchema.parseFromPulsarMessage(msg).ifPresent(schema -> {
                try {
                    if (schema.schema == TransitdataProperties.ProtobufSchema.GTFS_TripUpdate ||
                        schema.schema == TransitdataProperties.ProtobufSchema.GTFS_ServiceAlert ||
                        schema.schema == TransitdataProperties.ProtobufSchema.GTFS_VehiclePosition) {
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
        DatasetEntry entry = DatasetEntry.newEntry(msg, dataType);
        synchronized (inputQueue) {
            inputQueue.add(entry);
        }
    }

    public void close() {
        log.info("Closing poller");
        scheduler.shutdown();
    }

}
