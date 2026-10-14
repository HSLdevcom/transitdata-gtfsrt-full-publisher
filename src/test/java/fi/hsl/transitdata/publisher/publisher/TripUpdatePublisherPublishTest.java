package fi.hsl.transitdata.publisher.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import fi.hsl.transitdata.publisher.DatasetEntry;
import fi.hsl.transitdata.publisher.DatasetPublisher;
import fi.hsl.transitdata.publisher.TestData;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static fi.hsl.transitdata.publisher.TestData.*;
import static org.junit.Assert.*;

/**
 * Characterizes the cache, pruning and publishing behavior of {@link TripUpdatePublisher#publish(List)}.
 */
public class TripUpdatePublisherPublishTest {
    private static final String FULL = "full";
    private static final String GOOGLE = "google";
    private static final long MAX_AGE_SECS = 5 * 60;
    private static final long MAX_AGE_AFTER_START_SECS = 2 * 60 * 60;

    private TestData.RecordingSink sink;
    private TripUpdatePublisher publisher;

    @Before
    public void setUp() {
        sink = new TestData.RecordingSink();
        publisher = new TripUpdatePublisher(
                config("bundler.tripUpdate.contentMaxAge", "5 minutes", "bundler.tripUpdate.maxAgeAfterStart",
                        "2 hours", "bundler.tripUpdate.timezone", "UTC", "bundler.tripUpdate.fullContainerName", FULL,
                        "bundler.tripUpdate.googleContainerName", GOOGLE, "bundler.output.fileName", "tu.pb"),
                sink);
    }

    private static long nowSecs() {
        return System.currentTimeMillis() / 1000;
    }

    private static GtfsRealtime.TripDescriptor tripStartingAt(String tripId, String routeId, long startEpochSecs,
            ScheduleRelationship scheduleRelationship) {
        ZonedDateTime start = ZonedDateTime.ofInstant(java.time.Instant.ofEpochSecond(startEpochSecs), ZoneOffset.UTC);
        return trip(tripId, routeId, start.format(DateTimeFormatter.BASIC_ISO_DATE),
                start.format(DateTimeFormatter.ofPattern("HH:mm:ss")), scheduleRelationship);
    }

    private static DatasetEntry tu(String key, long eventTimeMs, GtfsRealtime.FeedEntity... entities) throws Exception {
        return entry(feedMessage(entities), key, eventTimeMs, DatasetPublisher.DataType.TripUpdate);
    }

    private static GtfsRealtime.FeedEntity scheduled(String tripId, String routeId, long stopTimeSecs) {
        return tripUpdateEntity(tripId, tripStartingAt(tripId, routeId, nowSecs(), ScheduleRelationship.SCHEDULED),
                stopTimeUpdate("1", stopTimeSecs, stopTimeSecs));
    }

    private static List<String> entityIds(TestData.RecordingSink.Put put) throws Exception {
        return put.feedMessage().getEntityList().stream().map(GtfsRealtime.FeedEntity::getId)
                .collect(Collectors.toList());
    }

    private void publish(DatasetEntry... entries) throws Exception {
        publisher.publish(new ArrayList<>(Arrays.asList(entries)));
    }

    @Test
    public void nothingIsPublishedWithoutNewMessages() throws Exception {
        publish(tu("a", 1, scheduled("a", "2550", nowSecs() + 60)));
        sink.puts.clear();

        publisher.publish(new ArrayList<>());

        assertTrue("cached entries are not republished without new messages", sink.puts.isEmpty());
    }

    @Test
    public void publishesFullAndGoogleDatasetsAsFullFeedMessages() throws Exception {
        long before = nowSecs();
        publish(tu("bus", 1, scheduled("bus", "2550", before + 60)));

        assertEquals(2, sink.puts.size());
        assertEquals(FULL, sink.puts.get(0).containerName);
        assertEquals(GOOGLE, sink.puts.get(1).containerName);
        for (TestData.RecordingSink.Put put : sink.puts) {
            assertEquals("tu.pb", put.fileName);
            GtfsRealtime.FeedHeader header = put.feedMessage().getHeader();
            assertEquals(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET, header.getIncrementality());
            assertTrue(header.getTimestamp() >= before && header.getTimestamp() <= nowSecs());
        }
        assertEquals(Collections.singletonList("bus"), entityIds(sink.puts.get(0)));
        assertEquals(Collections.emptyList(), entityIds(sink.puts.get(1)));
    }

    @Test
    public void googleDatasetHasCancellationsAndMetroAndTrainTrips() throws Exception {
        long future = nowSecs() + 600;
        publish(tu("bus", 1, scheduled("bus", "2550", future)), tu("metro", 2, scheduled("metro", "31M1", future)),
                tu("train", 3, scheduled("train", "3001P", future)),
                tu("cancelled-bus", 4, tripUpdateEntity("cancelled-bus",
                        tripStartingAt("cancelled-bus", "2550", nowSecs(), ScheduleRelationship.CANCELED))));

        assertEquals(new HashSet<>(Arrays.asList("bus", "metro", "train", "cancelled-bus")),
                new HashSet<>(entityIds(sink.lastPutTo(FULL))));
        assertEquals(new HashSet<>(Arrays.asList("metro", "train", "cancelled-bus")),
                new HashSet<>(entityIds(sink.lastPutTo(GOOGLE))));
    }

    @Test
    public void entitiesAreOrderedByEventTimeLatestFirst() throws Exception {
        long future = nowSecs() + 600;
        publish(tu("b", 2000, scheduled("b", "2550", future)), tu("c", 3000, scheduled("c", "2550", future)),
                tu("a", 1000, scheduled("a", "2550", future)));

        assertEquals(Arrays.asList("c", "b", "a"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void cacheKeepsTheLatestEventPerKey() throws Exception {
        long future = nowSecs() + 600;
        GtfsRealtime.FeedEntity newer = tripUpdateEntity("newer",
                tripStartingAt("t", "2550", nowSecs(), ScheduleRelationship.SCHEDULED),
                stopTimeUpdate("1", future, future));
        GtfsRealtime.FeedEntity older = tripUpdateEntity("older",
                tripStartingAt("t", "2550", nowSecs(), ScheduleRelationship.SCHEDULED),
                stopTimeUpdate("1", future, future));

        // Given out of order: sorted by event time before merging
        publish(tu("trip-key", 2000, newer), tu("trip-key", 1000, older));

        assertEquals(Collections.singletonList("newer"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void cacheIsKeptBetweenPublishes() throws Exception {
        long future = nowSecs() + 600;
        publish(tu("a", 1000, scheduled("a", "2550", future)));
        publish(tu("b", 2000, scheduled("b", "2550", future)));

        assertEquals(Arrays.asList("b", "a"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void entityIsRemovedWhenLatestStopTimeIsOlderThanMaxAge() throws Exception {
        long now = nowSecs();
        publish(tu("expired", 1, scheduled("expired", "2550", now - MAX_AGE_SECS - 60)),
                tu("recent", 2, scheduled("recent", "2550", now - MAX_AGE_SECS + 60)));

        assertEquals(Collections.singletonList("recent"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void latestOfArrivalAndDepartureOverAllStopsDecidesExpiry() throws Exception {
        long now = nowSecs();
        long old = now - MAX_AGE_SECS - 600;
        GtfsRealtime.FeedEntity entity = tripUpdateEntity("t",
                tripStartingAt("t", "2550", now, ScheduleRelationship.SCHEDULED), stopTimeUpdate("1", old, old),
                stopTimeUpdate("2", old, now - 10), stopTimeUpdate("3", old, old));

        publish(tu("t", 1, entity));

        assertEquals(Collections.singletonList("t"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void cancellationWithoutStopsExpiresRelativeToTripStart() throws Exception {
        long now = nowSecs();
        // expiration = start + maxAgeAfterStart; removed once expiration is older than contentMaxAge
        GtfsRealtime.FeedEntity kept = tripUpdateEntity("kept", tripStartingAt("kept", "2550",
                now - MAX_AGE_AFTER_START_SECS - MAX_AGE_SECS + 120, ScheduleRelationship.CANCELED));
        GtfsRealtime.FeedEntity removed = tripUpdateEntity("removed", tripStartingAt("removed", "2550",
                now - MAX_AGE_AFTER_START_SECS - MAX_AGE_SECS - 120, ScheduleRelationship.CANCELED));

        publish(tu("kept", 1, kept), tu("removed", 2, removed));

        assertEquals(Collections.singletonList("kept"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void tripWithOnlySkippedStopsExpiresRelativeToTripStart() throws Exception {
        long now = nowSecs();
        GtfsRealtime.FeedEntity entity = tripUpdateEntity("skipped",
                tripStartingAt("skipped", "2550", now - 60 * 60, ScheduleRelationship.SCHEDULED),
                stopTimeUpdate("1", StopTimeUpdate.ScheduleRelationship.SKIPPED),
                stopTimeUpdate("2", StopTimeUpdate.ScheduleRelationship.NO_DATA));

        publish(tu("skipped", 1, entity));

        assertEquals(Collections.singletonList("skipped"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void tripWithOnlyNoDataStopsIsRemoved() throws Exception {
        GtfsRealtime.FeedEntity entity = tripUpdateEntity("no-data",
                tripStartingAt("no-data", "2550", nowSecs(), ScheduleRelationship.SCHEDULED),
                stopTimeUpdate("1", StopTimeUpdate.ScheduleRelationship.NO_DATA));

        publish(tu("no-data", 1, entity));

        assertTrue(entityIds(sink.lastPutTo(FULL)).isEmpty());
    }

    @Test
    public void scheduledTripWithoutStopsIsRemoved() throws Exception {
        GtfsRealtime.FeedEntity entity = tripUpdateEntity("empty",
                tripStartingAt("empty", "2550", nowSecs(), ScheduleRelationship.SCHEDULED));

        publish(tu("empty", 1, entity));

        assertTrue(entityIds(sink.lastPutTo(FULL)).isEmpty());
    }

    @Test
    public void cancellationTimestampIsSetToPublishTime() throws Exception {
        long before = nowSecs();
        publish(tu("c", 1, tripUpdateEntity("c", tripStartingAt("c", "2550", before, ScheduleRelationship.CANCELED))),
                tu("s", 2, scheduled("s", "31M1", before + 600)));

        for (String container : Arrays.asList(FULL, GOOGLE)) {
            Map<String, GtfsRealtime.FeedEntity> byId = sink.lastPutTo(container).feedMessage().getEntityList().stream()
                    .collect(Collectors.toMap(GtfsRealtime.FeedEntity::getId, e -> e));
            long cancelledTimestamp = byId.get("c").getTripUpdate().getTimestamp();
            assertTrue(container, cancelledTimestamp >= before && cancelledTimestamp <= nowSecs());
            assertEquals(container, 0L, byId.get("s").getTripUpdate().getTimestamp());
        }
    }

    @Test
    public void allEntitiesOfAMultiEntityMessageArePublished() throws Exception {
        long future = nowSecs() + 600;
        publish(tu("multi", 1, scheduled("e1", "2550", future), scheduled("e2", "2550", future)));

        assertEquals(Arrays.asList("e1", "e2"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void multiEntityMessageIsKeptWhileAnyEntityIsRecent() throws Exception {
        long now = nowSecs();
        publish(tu("multi", 1, scheduled("old", "2550", now - MAX_AGE_SECS - 600),
                scheduled("recent", "2550", now + 600)));

        assertEquals(Arrays.asList("old", "recent"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void entriesWithoutKeyShareOneCacheSlot() throws Exception {
        long future = nowSecs() + 600;
        publish(tu(null, 1000, scheduled("first", "2550", future)),
                tu(null, 2000, scheduled("second", "2550", future)));

        assertEquals(Collections.singletonList("second"), entityIds(sink.lastPutTo(FULL)));
    }

    @Test
    public void expirationTimeHandlesStartTimesPastMidnight() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(trip("t", "2550", "20240101", "25:10:30", ScheduleRelationship.CANCELED)).build();

        long expected = ZonedDateTime.of(2024, 1, 2, 1, 10, 30, 0, ZoneId.of("Europe/Helsinki")).plusSeconds(60)
                .toEpochSecond();
        assertEquals(expected, TripUpdatePublisher.getExpirationTime(tripUpdate, ZoneId.of("Europe/Helsinki"), 60));
    }

    @Test
    public void shouldUseExpirationTime() {
        GtfsRealtime.TripDescriptor scheduledTrip = trip("t", "2550", "20240101", "10:00:00",
                ScheduleRelationship.SCHEDULED);
        GtfsRealtime.TripDescriptor cancelledTrip = trip("t", "2550", "20240101", "10:00:00",
                ScheduleRelationship.CANCELED);

        assertTrue(TripUpdatePublisher
                .shouldUseExpirationTime(GtfsRealtime.TripUpdate.newBuilder().setTrip(cancelledTrip).build()));
        // allMatch over no stop time updates is true
        assertTrue(TripUpdatePublisher
                .shouldUseExpirationTime(GtfsRealtime.TripUpdate.newBuilder().setTrip(scheduledTrip).build()));
        assertTrue(
                TripUpdatePublisher.shouldUseExpirationTime(GtfsRealtime.TripUpdate.newBuilder().setTrip(scheduledTrip)
                        .addStopTimeUpdate(stopTimeUpdate("1", StopTimeUpdate.ScheduleRelationship.SKIPPED)).build()));
        assertFalse(
                TripUpdatePublisher.shouldUseExpirationTime(GtfsRealtime.TripUpdate.newBuilder().setTrip(scheduledTrip)
                        .addStopTimeUpdate(stopTimeUpdate("1", StopTimeUpdate.ScheduleRelationship.SKIPPED))
                        .addStopTimeUpdate(stopTimeUpdate("2", 100, 100)).build()));
        assertFalse(TripUpdatePublisher.shouldUseExpirationTime(GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(cancelledTrip).addStopTimeUpdate(stopTimeUpdate("2", 100, 100)).build()));
    }

    @Test
    public void hasDataForEveryStopScheduleRelationship() {
        Map<StopTimeUpdate.ScheduleRelationship, Boolean> expected = new EnumMap<>(
                StopTimeUpdate.ScheduleRelationship.class);
        expected.put(StopTimeUpdate.ScheduleRelationship.SCHEDULED, true);
        expected.put(StopTimeUpdate.ScheduleRelationship.SKIPPED, true);
        expected.put(StopTimeUpdate.ScheduleRelationship.NO_DATA, false);
        assertEquals("expected table must cover every StopTimeUpdate.ScheduleRelationship",
                StopTimeUpdate.ScheduleRelationship.values().length, expected.size());

        GtfsRealtime.TripDescriptor scheduledTrip = trip("t", "2550", "20240101", "10:00:00",
                ScheduleRelationship.SCHEDULED);
        for (StopTimeUpdate.ScheduleRelationship relationship : StopTimeUpdate.ScheduleRelationship.values()) {
            GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder().setTrip(scheduledTrip)
                    .addStopTimeUpdate(stopTimeUpdate("1", relationship)).build();
            assertEquals(relationship.name(), expected.get(relationship), TripUpdatePublisher.hasData(tripUpdate));
        }

        // No schedule relationship defaults to SCHEDULED; cancelled trips always have data
        assertTrue(TripUpdatePublisher.hasData(GtfsRealtime.TripUpdate.newBuilder().setTrip(scheduledTrip)
                .addStopTimeUpdate(StopTimeUpdate.newBuilder().setStopId("1")).build()));
        assertTrue(TripUpdatePublisher.hasData(GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(trip("t", "2550", "20240101", "10:00:00", ScheduleRelationship.CANCELED)).build()));
    }

    @Test
    public void googleFilterForEveryTripScheduleRelationship() {
        Map<ScheduleRelationship, Boolean> busIncluded = new EnumMap<>(ScheduleRelationship.class);
        for (ScheduleRelationship relationship : ScheduleRelationship.values()) {
            busIncluded.put(relationship, relationship == ScheduleRelationship.CANCELED);
        }

        for (ScheduleRelationship relationship : ScheduleRelationship.values()) {
            GtfsRealtime.FeedEntity bus = tripUpdateEntity("bus",
                    trip("t", "2550", "20240101", "10:00:00", relationship));
            GtfsRealtime.FeedEntity metro = tripUpdateEntity("metro",
                    trip("t", "31M1", "20240101", "10:00:00", relationship));
            assertEquals(relationship.name(), busIncluded.get(relationship) ? 1 : 0,
                    TripUpdatePublisher.filterTripUpdatesForGoogle(Collections.singletonList(bus)).size());
            assertEquals(relationship.name(), 1,
                    TripUpdatePublisher.filterTripUpdatesForGoogle(Collections.singletonList(metro)).size());
        }

        // Entities without schedule relationship or route id are always dropped
        GtfsRealtime.FeedEntity noRelationship = GtfsRealtime.FeedEntity.newBuilder().setId("x")
                .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                        .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setRouteId("31M1")))
                .build();
        GtfsRealtime.FeedEntity noRoute = GtfsRealtime.FeedEntity.newBuilder().setId("y")
                .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder().setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setScheduleRelationship(ScheduleRelationship.CANCELED)))
                .build();
        assertTrue(TripUpdatePublisher.filterTripUpdatesForGoogle(Arrays.asList(noRelationship, noRoute)).isEmpty());
    }
}
