package fi.hsl.transitdata.publisher.publisher;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.transitdata.publisher.DatasetEntry;
import fi.hsl.transitdata.publisher.DatasetPublisher;
import fi.hsl.transitdata.publisher.TestData;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static fi.hsl.transitdata.publisher.TestData.*;
import static org.junit.Assert.*;

/**
 * Characterizes the cache, pruning and publishing behavior of {@link VehiclePositionPublisher#publish(List)}.
 */
public class VehiclePositionPublisherPublishTest {
    private static final String FULL = "vp-full";
    private static final String BUS_TRAM = "vp-bustram";
    private static final String TRAIN_METRO = "vp-trainmetro";

    private TestData.RecordingSink sink;
    private VehiclePositionPublisher publisher;

    @Before
    public void setUp() {
        sink = new TestData.RecordingSink();
        publisher = new VehiclePositionPublisher(config("bundler.vehiclePosition.contentMaxAge", "2 minutes",
                "bundler.vehiclePosition.fullContainerName", FULL, "bundler.vehiclePosition.busTramContainerName",
                BUS_TRAM, "bundler.vehiclePosition.trainMetroContainerName", TRAIN_METRO, "bundler.output.fileName",
                "vp.pb"), sink);
    }

    private static long nowSecs() {
        return System.currentTimeMillis() / 1000;
    }

    private static DatasetEntry vp(long eventTimeMs, GtfsRealtime.FeedEntity... entities) throws Exception {
        return entry(feedMessage(entities), "ignored-key", eventTimeMs, DatasetPublisher.DataType.VehiclePosition);
    }

    private void publish(DatasetEntry... entries) throws Exception {
        publisher.publish(new ArrayList<>(Arrays.asList(entries)));
    }

    private Set<String> vehicleIds(String container) throws Exception {
        return sink.lastPutTo(container).feedMessage().getEntityList().stream()
                .map(e -> e.getVehicle().getVehicle().getId()).collect(Collectors.toSet());
    }

    private GtfsRealtime.FeedEntity onlyEntity(String container) throws Exception {
        GtfsRealtime.FeedMessage feedMessage = sink.lastPutTo(container).feedMessage();
        assertEquals(1, feedMessage.getEntityCount());
        return feedMessage.getEntity(0);
    }

    @Test
    public void nothingIsPublishedWithoutNewMessages() throws Exception {
        publish(vp(1, vehiclePositionEntity("bus", "2550", nowSecs())));
        sink.puts.clear();

        publisher.publish(new ArrayList<>());

        assertTrue(sink.puts.isEmpty());
    }

    @Test
    public void publishesFullBusTramAndTrainMetroDatasets() throws Exception {
        long now = nowSecs();
        publish(vp(1, vehiclePositionEntity("bus", "2550", now)), vp(2, vehiclePositionEntity("tram", "1009", now)),
                vp(3, vehiclePositionEntity("metro", "31M1", now)),
                vp(4, vehiclePositionEntity("train", "3001P", now)));

        assertEquals(Arrays.asList(FULL, BUS_TRAM, TRAIN_METRO),
                sink.puts.stream().map(p -> p.containerName).collect(Collectors.toList()));
        for (TestData.RecordingSink.Put put : sink.puts) {
            assertEquals("vp.pb", put.fileName);
            GtfsRealtime.FeedHeader header = put.feedMessage().getHeader();
            assertEquals(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET, header.getIncrementality());
            assertTrue(header.getTimestamp() >= now && header.getTimestamp() <= nowSecs());
        }
        assertEquals(new HashSet<>(Arrays.asList("bus", "tram", "metro", "train")), vehicleIds(FULL));
        assertEquals(new HashSet<>(Arrays.asList("bus", "tram")), vehicleIds(BUS_TRAM));
        assertEquals(new HashSet<>(Arrays.asList("metro", "train")), vehicleIds(TRAIN_METRO));
    }

    @Test
    public void newerVehicleTimestampReplacesCachedPosition() throws Exception {
        long now = nowSecs();
        publish(vp(1, vehiclePositionEntity("v1", "2550", now - 10)));
        publish(vp(2, vehiclePositionEntity("v1", "2550", now)));

        assertEquals(now, onlyEntity(FULL).getVehicle().getTimestamp());
    }

    @Test
    public void olderVehicleTimestampDoesNotReplaceCachedPosition() throws Exception {
        long now = nowSecs();
        publish(vp(1, vehiclePositionEntity("v1", "2550", now)));
        publish(vp(2, vehiclePositionEntity("v1", "2550", now - 10)));

        assertEquals(now, onlyEntity(FULL).getVehicle().getTimestamp());
    }

    @Test
    public void equalVehicleTimestampIsReplacedByTheLaterEvent() throws Exception {
        long now = nowSecs();
        GtfsRealtime.FeedEntity first = vehiclePositionEntity("v1", "2550", now).toBuilder().setId("first").build();
        GtfsRealtime.FeedEntity second = vehiclePositionEntity("v1", "1009", now).toBuilder().setId("second").build();

        // Sorted by event time before merging
        publish(vp(2000, second), vp(1000, first));

        assertEquals("second", onlyEntity(FULL).getId());
    }

    @Test
    public void positionsOlderThanMaxAgeArePruned() throws Exception {
        long now = nowSecs();
        publish(vp(1, vehiclePositionEntity("old", "2550", now - 3 * 60)),
                vp(2, vehiclePositionEntity("recent", "2550", now - 60)),
                vp(3, vehiclePositionEntity("future", "2550", now + 60)));

        assertEquals(new HashSet<>(Arrays.asList("recent", "future")), vehicleIds(FULL));
    }

    @Test
    public void emptyDatasetsArePublishedWhenEverythingIsPruned() throws Exception {
        publish(vp(1, vehiclePositionEntity("old", "2550", nowSecs() - 3 * 60)));

        assertEquals(3, sink.puts.size());
        for (TestData.RecordingSink.Put put : sink.puts) {
            assertEquals(0, put.feedMessage().getEntityCount());
        }
    }

    @Test
    public void cacheIsKeptBetweenPublishes() throws Exception {
        long now = nowSecs();
        publish(vp(1, vehiclePositionEntity("a", "2550", now)));
        publish(vp(2, vehiclePositionEntity("b", "2550", now)));

        assertEquals(new HashSet<>(Arrays.asList("a", "b")), vehicleIds(FULL));
    }

    @Test
    public void onlyTheFirstEntityOfAMessageIsUsed() throws Exception {
        long now = nowSecs();
        publish(vp(1, vehiclePositionEntity("first", "2550", now), vehiclePositionEntity("second", "2550", now)));

        assertEquals(Collections.singleton("first"), vehicleIds(FULL));
    }

    @Test
    public void messagesWithoutEntitiesAreSkipped() throws Exception {
        publish(vp(1));

        assertEquals(3, sink.puts.size());
        assertEquals(0, sink.lastPutTo(FULL).feedMessage().getEntityCount());
    }

    @Test
    public void vehiclesWithoutIdShareOneCacheSlot() throws Exception {
        long now = nowSecs();
        publish(vp(1, vehiclePositionEntity(null, "2550", now - 5)), vp(2, vehiclePositionEntity(null, "1009", now)));

        assertEquals("1009", onlyEntity(FULL).getVehicle().getTrip().getRouteId());
    }
}
