package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.ProtobufSchema;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static fi.hsl.transitdata.publisher.TestData.*;
import static org.junit.Assert.*;

/**
 * End-to-end: Pulsar topic → the application wired as in {@link Main} → GTFS-RT full datasets on the local sink.
 */
public class GtfsrtFullPublisherIT {
    // Matches the pulsar-client version of transitdata-common 2.0.3
    private static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:3.0.7");

    @ClassRule
    public static final PulsarContainer PULSAR = new PulsarContainer(PULSAR_IMAGE);

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private static PulsarClient client;
    private static PulsarAdmin admin;

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private String topic;
    private String subscription;
    private int healthPort;

    @BeforeClass
    public static void connect() throws Exception {
        client = PulsarClient.builder().serviceUrl(PULSAR.getPulsarBrokerUrl()).build();
        admin = PulsarAdmin.builder().serviceHttpUrl(PULSAR.getHttpServiceUrl()).build();
    }

    @AfterClass
    public static void disconnect() throws Exception {
        if (client != null) {
            client.close();
        }
        if (admin != null) {
            admin.close();
        }
    }

    @Before
    public void uniqueNames() throws Exception {
        String id = UUID.randomUUID().toString();
        topic = "persistent://public/default/gtfsrt-" + id;
        subscription = "sub-" + id;
        try (ServerSocket socket = new ServerSocket(0)) {
            healthPort = socket.getLocalPort();
        }
    }

    @After
    public void closeAll() {
        Collections.reverse(closeables);
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    private void launch(String dataType, String... extra) throws Exception {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("pulsar.host", PULSAR.getHost());
        overrides.put("pulsar.port", PULSAR.getMappedPort(PulsarContainer.BROKER_PORT));
        overrides.put("pulsar.consumer.topic", topic);
        overrides.put("pulsar.consumer.subscription", subscription);
        overrides.put("health.enabled", true);
        overrides.put("health.port", healthPort);
        overrides.put("bundler.dataType", dataType);
        overrides.put("bundler.dumpInterval", "1 second");
        overrides.put("bundler.output.destination", "local");
        overrides.put("bundler.output.local.path", tmp.getRoot().getPath());
        overrides.put("bundler.output.fileName", "out.pb");
        for (int i = 0; i < extra.length; i += 2) {
            overrides.put(extra[i], extra[i + 1]);
        }
        Config config = config(overrides);

        // As in Main
        PulsarApplication app = PulsarApplication.newInstance(config);
        MessageProcessor processor = MessageProcessor.newInstance(app);
        closeables.add(app);
        closeables.add(processor::close);

        Thread thread = new Thread(() -> {
            try {
                app.launchWithHandler(processor);
            } catch (Exception ignored) {
                // closed by the test
            }
        }, "pulsar-app-" + dataType);
        thread.setDaemon(true);
        thread.start();
    }

    private Producer<byte[]> producer() throws Exception {
        Producer<byte[]> producer = client.newProducer().topic(topic).create();
        closeables.add(producer);
        return producer;
    }

    private static void send(Producer<byte[]> producer, GtfsRealtime.FeedMessage feedMessage, String key,
            Long eventTimeMs, ProtobufSchema schema) throws Exception {
        TypedMessageBuilder<byte[]> builder = producer.newMessage().value(feedMessage.toByteArray());
        if (key != null) {
            builder.key(key);
        }
        if (eventTimeMs != null) {
            builder.eventTime(eventTimeMs);
        }
        if (schema != null) {
            builder.property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.toString());
        }
        builder.send();
    }

    /**
     * Polls the published file until it satisfies the condition.
     */
    private GtfsRealtime.FeedMessage awaitDataset(String container,
            java.util.function.Predicate<GtfsRealtime.FeedMessage> condition) throws Exception {
        Path file = tmp.getRoot().toPath().resolve(container).resolve("out.pb");
        long deadline = System.currentTimeMillis() + 30_000;
        GtfsRealtime.FeedMessage last = null;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(file)) {
                try {
                    last = GtfsRealtime.FeedMessage.parseFrom(Files.readAllBytes(file));
                    if (condition.test(last)) {
                        return last;
                    }
                } catch (Exception partialWrite) {
                    // file is being rewritten, retry
                }
            }
            Thread.sleep(100);
        }
        fail("dataset in " + container + " did not match in time, last: " + last);
        return null;
    }

    private static Set<String> entityIds(GtfsRealtime.FeedMessage feedMessage) {
        return feedMessage.getEntityList().stream().map(GtfsRealtime.FeedEntity::getId).collect(Collectors.toSet());
    }

    private void awaitNoBacklog() throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        long backlog = -1;
        while (System.currentTimeMillis() < deadline) {
            backlog = admin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog();
            if (backlog == 0) {
                return;
            }
            Thread.sleep(200);
        }
        fail("subscription backlog is " + backlog);
    }

    private int healthStatus() throws Exception {
        HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:" + healthPort + "/health")
                .openConnection();
        try {
            return connection.getResponseCode();
        } finally {
            connection.disconnect();
        }
    }

    @Test
    public void tripUpdatesArePublishedToFullAndGoogleDatasets() throws Exception {
        launch("TripUpdate", "bundler.tripUpdate.timezone", "UTC");
        Producer<byte[]> producer = producer();
        long now = System.currentTimeMillis() / 1000;
        String today = java.time.LocalDate.now(java.time.ZoneOffset.UTC)
                .format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
        GtfsRealtime.TripDescriptor.ScheduleRelationship scheduled = GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED;

        send(producer,
                feedMessage(tripUpdateEntity("bus-v1", trip("bus", "2550", today, "23:59:00", scheduled),
                        stopTimeUpdate("1", now + 600, now + 600))),
                "bus-key", now * 1000, ProtobufSchema.GTFS_TripUpdate);
        send(producer,
                feedMessage(tripUpdateEntity("bus-v2", trip("bus", "2550", today, "23:59:00", scheduled),
                        stopTimeUpdate("1", now + 600, now + 600))),
                "bus-key", now * 1000 + 1, ProtobufSchema.GTFS_TripUpdate);
        send(producer,
                feedMessage(tripUpdateEntity("metro", trip("metro", "31M1", today, "23:59:00", scheduled),
                        stopTimeUpdate("1", now + 600, now + 600))),
                "metro-key", now * 1000, ProtobufSchema.GTFS_TripUpdate);
        send(producer,
                feedMessage(tripUpdateEntity("cancelled",
                        trip("cancelled", "2550", today, "23:59:00",
                                GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED))),
                "cancelled-key", now * 1000, ProtobufSchema.GTFS_TripUpdate);
        // Not a GTFS-RT schema: acked and ignored
        send(producer,
                feedMessage(tripUpdateEntity("ignored", trip("x", "2550", today, "23:59:00", scheduled),
                        stopTimeUpdate("1", now + 600, now + 600))),
                "ignored-key", now * 1000, ProtobufSchema.InternalMessagesStopEstimate);

        Set<String> expectedFull = new HashSet<>(Arrays.asList("bus-v2", "metro", "cancelled"));
        GtfsRealtime.FeedMessage full = awaitDataset("trip-updates", fm -> entityIds(fm).equals(expectedFull));
        assertEquals(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET, full.getHeader().getIncrementality());

        Set<String> expectedGoogle = new HashSet<>(Arrays.asList("metro", "cancelled"));
        awaitDataset("trip-updates-for-google", fm -> entityIds(fm).equals(expectedGoogle));

        awaitNoBacklog();
        assertEquals(200, healthStatus());
    }

    @Test
    public void vehiclePositionsArePublishedToThreeDatasets() throws Exception {
        launch("VehiclePosition");
        Producer<byte[]> producer = producer();
        long now = System.currentTimeMillis() / 1000;

        send(producer, feedMessage(vehiclePositionEntity("bus", "2550", now)), "k1", now * 1000,
                ProtobufSchema.GTFS_VehiclePosition);
        send(producer, feedMessage(vehiclePositionEntity("train", "3001P", now)), "k2", now * 1000,
                ProtobufSchema.GTFS_VehiclePosition);
        send(producer, feedMessage(vehiclePositionEntity("stale", "2550", now - 3 * 60)), "k3", now * 1000,
                ProtobufSchema.GTFS_VehiclePosition);

        awaitDataset("vehicle-positions",
                fm -> entityIds(fm).equals(new HashSet<>(Arrays.asList("vp-bus-" + now, "vp-train-" + now))));
        awaitDataset("vehicle-positions-bustram", fm -> entityIds(fm).equals(Collections.singleton("vp-bus-" + now)));
        awaitDataset("vehicle-positions-trainmetro",
                fm -> entityIds(fm).equals(Collections.singleton("vp-train-" + now)));
        awaitNoBacklog();
    }

    @Test
    public void latestServiceAlertIsPublishedAsIs() throws Exception {
        launch("ServiceAlert");
        Producer<byte[]> producer = producer();
        GtfsRealtime.FeedMessage older = feedMessage(alertEntity("older", "old"));
        GtfsRealtime.FeedMessage latest = feedMessage(alertEntity("latest", "new"));

        send(producer, latest, null, 2000L, ProtobufSchema.GTFS_ServiceAlert);
        send(producer, older, null, 1000L, ProtobufSchema.GTFS_ServiceAlert);

        GtfsRealtime.FeedMessage published = awaitDataset("service-alerts", fm -> fm.getEntityCount() > 0);
        assertEquals(latest, published);
        awaitNoBacklog();
    }

    @Test
    public void serviceAlertWithoutEventTimeIsPublished() throws Exception {
        launch("ServiceAlert");
        Producer<byte[]> producer = producer();
        GtfsRealtime.FeedMessage alert = feedMessage(alertEntity("no-event-time", "text"));

        send(producer, alert, null, null, ProtobufSchema.GTFS_ServiceAlert);

        assertEquals(alert, awaitDataset("service-alerts", fm -> fm.getEntityCount() > 0));
    }
}
