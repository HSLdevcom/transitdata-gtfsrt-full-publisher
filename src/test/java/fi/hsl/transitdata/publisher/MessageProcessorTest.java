package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.health.HealthServer;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties.ProtobufSchema;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static fi.hsl.transitdata.publisher.TestData.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Characterizes {@link MessageProcessor} wired as in {@link Main}, with mocked Pulsar and a local sink.
 */
public class MessageProcessorTest {
    private static final GtfsRealtime.FeedMessage FEED_MESSAGE = feedMessage(alertEntity("a1", "text"));

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private PulsarApplication app;
    private PulsarApplicationContext context;
    private Consumer<byte[]> consumer;
    private final List<MessageProcessor> processors = new ArrayList<>();

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        app = mock(PulsarApplication.class);
        context = mock(PulsarApplicationContext.class);
        consumer = mock(Consumer.class);
        when(app.getContext()).thenReturn(context);
        when(context.getConsumer()).thenReturn(consumer);
        when(consumer.getTopic()).thenReturn("topic");
        when(consumer.acknowledgeAsync(any(Message.class))).thenReturn(CompletableFuture.completedFuture(null));
    }

    @After
    public void tearDown() {
        processors.forEach(MessageProcessor::close);
    }

    private MessageProcessor processor(String dataType, String dumpInterval, String... extra) throws Exception {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("bundler.dataType", dataType);
        overrides.put("bundler.dumpInterval", dumpInterval);
        overrides.put("bundler.output.destination", "local");
        overrides.put("bundler.output.local.path", tmp.getRoot().getPath());
        overrides.put("bundler.output.fileName", "out.pb");
        for (int i = 0; i < extra.length; i += 2) {
            overrides.put(extra[i], extra[i + 1]);
        }
        Config config = config(overrides);
        when(context.getConfig()).thenReturn(config);
        MessageProcessor processor = MessageProcessor.newInstance(app);
        processors.add(processor);
        return processor;
    }

    private static Message<byte[]> message(ProtobufSchema schema) {
        return pulsarMessage(FEED_MESSAGE.toByteArray(), "key", 1000L, schema);
    }

    private static Path awaitFile(Path file) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!Files.exists(file) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        return file;
    }

    @Test
    public void onlyGtfsSchemasAreQueued() throws Exception {
        Set<ProtobufSchema> accepted = EnumSet.of(ProtobufSchema.GTFS_TripUpdate, ProtobufSchema.GTFS_ServiceAlert,
                ProtobufSchema.GTFS_VehiclePosition);
        Map<ProtobufSchema, Boolean> expectedQueued = new EnumMap<>(ProtobufSchema.class);
        for (ProtobufSchema schema : ProtobufSchema.values()) {
            expectedQueued.put(schema, accepted.contains(schema));
        }
        assertEquals("expected table must cover every ProtobufSchema", ProtobufSchema.values().length,
                expectedQueued.size());

        MessageProcessor processor = processor("TripUpdate", "1 hour");
        for (ProtobufSchema schema : ProtobufSchema.values()) {
            int before = processor.inputQueue.size();
            Message<byte[]> msg = message(schema);

            processor.handleMessage(msg);

            assertEquals(schema.name(), expectedQueued.get(schema) ? before + 1 : before, processor.inputQueue.size());
            verify(consumer).acknowledgeAsync(msg);
        }
    }

    @Test
    public void gtfsSchemaIsQueuedRegardlessOfConfiguredDataType() throws Exception {
        MessageProcessor processor = processor("ServiceAlert", "1 hour");

        processor.handleMessage(message(ProtobufSchema.GTFS_TripUpdate));

        assertEquals(1, processor.inputQueue.size());
        // Parsed as the configured data type: no id for service alerts
        assertNull(processor.inputQueue.get(0).getId());
    }

    @Test
    public void queuedTripUpdateUsesMessageKeyAsId() throws Exception {
        MessageProcessor processor = processor("TripUpdate", "1 hour");

        processor.handleMessage(message(ProtobufSchema.GTFS_TripUpdate));

        assertEquals("key", processor.inputQueue.get(0).getId());
        assertEquals(1000L, processor.inputQueue.get(0).getEventTimeUtcMs());
    }

    @Test
    public void messageWithoutSchemaIsIgnoredAndAcked() throws Exception {
        MessageProcessor processor = processor("TripUpdate", "1 hour");
        Message<byte[]> msg = message(null);

        processor.handleMessage(msg);

        assertTrue(processor.inputQueue.isEmpty());
        verify(consumer).acknowledgeAsync(msg);
    }

    @Test
    public void unknownSchemaIsIgnoredAndAcked() throws Exception {
        MessageProcessor processor = processor("TripUpdate", "1 hour");
        Message<byte[]> msg = message(null);
        when(msg.getProperty("protobuf-schema")).thenReturn("no-such-schema");

        processor.handleMessage(msg);

        assertTrue(processor.inputQueue.isEmpty());
        verify(consumer).acknowledgeAsync(msg);
    }

    @Test
    public void malformedPayloadIsDroppedAndAcked() throws Exception {
        MessageProcessor processor = processor("TripUpdate", "1 hour");
        Message<byte[]> msg = pulsarMessage(new byte[]{(byte) 0xff, 1, 2}, "key", 1L, ProtobufSchema.GTFS_TripUpdate);

        processor.handleMessage(msg);

        assertTrue(processor.inputQueue.isEmpty());
        verify(consumer).acknowledgeAsync(msg);
    }

    @Test
    public void queuedMessagesArePublishedAtDumpIntervalAndQueueIsCleared() throws Exception {
        MessageProcessor processor = processor("ServiceAlert", "1 second", "bundler.serviceAlert.containerName",
                "alerts");

        processor.handleMessage(message(ProtobufSchema.GTFS_ServiceAlert));

        Path file = awaitFile(tmp.getRoot().toPath().resolve("alerts/out.pb"));
        assertArrayEquals(FEED_MESSAGE.toByteArray(), Files.readAllBytes(file));
        assertTrue(processor.inputQueue.isEmpty());
        verify(app, never()).close();
    }

    @Test
    public void publishFailureClosesTheApplication() throws Exception {
        // A regular file where the container directory should be makes LocalSink.put fail
        Files.write(tmp.getRoot().toPath().resolve("alerts"), new byte[]{0});
        MessageProcessor processor = processor("ServiceAlert", "1 second", "bundler.serviceAlert.containerName",
                "alerts");

        processor.handleMessage(message(ProtobufSchema.GTFS_ServiceAlert));

        verify(app, timeout(10_000)).close();
        assertTrue(processor.scheduler.isShutdown());
    }

    @Test(expected = IllegalArgumentException.class)
    public void dumpIntervalBelowOneSecondIsRejected() throws Exception {
        // The interval is truncated to whole seconds and a zero period is invalid
        processor("TripUpdate", "500 milliseconds");
    }

    @Test
    public void healthCheckIsNotRegisteredWithoutHealthServer() throws Exception {
        when(context.getHealthServer()).thenReturn(null);
        MessageProcessor processor = processor("TripUpdate", "1 hour");
        processor.handleMessage(message(ProtobufSchema.GTFS_TripUpdate));
    }

    @Test
    public void unhealthyWhenReceivedDataIsNotPublishedWithinTimeout() throws Exception {
        HealthServer healthServer = mock(HealthServer.class);
        when(context.getHealthServer()).thenReturn(healthServer);
        MessageProcessor processor = processor("ServiceAlert", "1 second", "health.unhealthyTimeout",
                "300 milliseconds", "bundler.serviceAlert.containerName", "alerts");

        ArgumentCaptor<BooleanSupplier> check = ArgumentCaptor.forClass(BooleanSupplier.class);
        verify(healthServer).addCheck(check.capture());
        assertTrue("healthy right after start", check.getValue().getAsBoolean());

        Thread.sleep(400);
        assertTrue("not receiving anything keeps the service healthy", check.getValue().getAsBoolean());

        processor.handleMessage(message(ProtobufSchema.GTFS_ServiceAlert));
        assertFalse("received but not yet published", check.getValue().getAsBoolean());

        awaitFile(tmp.getRoot().toPath().resolve("alerts/out.pb"));
        long deadline = System.currentTimeMillis() + 5_000;
        while (!check.getValue().getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue("healthy again after publishing", check.getValue().getAsBoolean());
    }

    @Test
    public void closeStopsTheScheduler() throws Exception {
        MessageProcessor processor = processor("TripUpdate", "1 hour");
        processor.close();
        assertTrue(processor.scheduler.isShutdown());
    }
}
