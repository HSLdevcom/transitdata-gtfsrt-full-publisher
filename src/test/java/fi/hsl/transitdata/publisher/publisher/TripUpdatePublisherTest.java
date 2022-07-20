package fi.hsl.transitdata.publisher.publisher;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static org.junit.Assert.*;

public class TripUpdatePublisherTest {
    @Test
    public void testGetExpirationTimeForCancellation() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
            .setTimestamp(0)
            .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                .setTripId("trip_1")
                .setDirectionId(1)
                .setStartDate("20000101")
                .setStartTime("06:45:00")
                .setRouteId("route_1")
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED))
            .build();

        long maxAge = 2 * 60 * 60;
        ZonedDateTime dateTime = ZonedDateTime.of(2000, 1, 1, 6, 45, 0, 0, ZoneId.of("UTC")).plusSeconds(maxAge);

        assertEquals(dateTime.toEpochSecond(), TripUpdatePublisher.getExpirationTime(tripUpdate, ZoneId.of("UTC"), maxAge));
    }

    @Test
    public void testTimestampIsUpdatedForCancellation() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setTripId("trip_1")
                        .setDirectionId(1)
                        .setStartDate("20000101")
                        .setStartTime("06:45:00")
                        .setRouteId("route_1")
                        .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED))
                .build();

        GtfsRealtime.FeedEntity feedEntity = GtfsRealtime.FeedEntity.newBuilder().setTripUpdate(tripUpdate).setId("entity_1").build();

        assertEquals(10000, TripUpdatePublisher.updateCancellationTimestamp(feedEntity, 10000).getTripUpdate().getTimestamp());
    }

    @Test
    public void testTimestampIsNotUpdatedForScheduledTU() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setTripId("trip_1")
                        .setDirectionId(1)
                        .setStartDate("20000101")
                        .setStartTime("06:45:00")
                        .setRouteId("route_1")
                        .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED))
                .build();

        GtfsRealtime.FeedEntity feedEntity = GtfsRealtime.FeedEntity.newBuilder().setTripUpdate(tripUpdate).setId("entity_1").build();

        assertEquals(0, TripUpdatePublisher.updateCancellationTimestamp(feedEntity, 10000).getTripUpdate().getTimestamp());
    }

    @Test
    public void testScheduledBusTripUpdatesAreFilteredForGoogle() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setTripId("trip_1")
                        .setDirectionId(1)
                        .setStartDate("20000101")
                        .setStartTime("06:45:00")
                        .setRouteId("2550")
                        .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED))
                .build();

        GtfsRealtime.FeedEntity feedEntity = GtfsRealtime.FeedEntity.newBuilder().setTripUpdate(tripUpdate).setId("entity_1").build();

        assertEquals(0, TripUpdatePublisher.filterTripUpdatesForGoogle(Arrays.asList(feedEntity)).size());
    }

    @Test
    public void testCanceledBusTripUpdatesAreNotFilteredForGoogle() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setTripId("trip_1")
                        .setDirectionId(1)
                        .setStartDate("20000101")
                        .setStartTime("06:45:00")
                        .setRouteId("2550")
                        .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED))
                .build();

        GtfsRealtime.FeedEntity feedEntity = GtfsRealtime.FeedEntity.newBuilder().setTripUpdate(tripUpdate).setId("entity_1").build();

        assertEquals(1, TripUpdatePublisher.filterTripUpdatesForGoogle(Arrays.asList(feedEntity)).size());
    }

    @Test
    public void testScheduledMetroAndTrainTripUpdatesAreNotFilteredForGoogle() {
        GtfsRealtime.TripUpdate tripUpdate1 = GtfsRealtime.TripUpdate.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setTripId("trip_1")
                        .setDirectionId(1)
                        .setStartDate("20000101")
                        .setStartTime("06:45:00")
                        .setRouteId("31M1")
                        .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED))
                .build();

        GtfsRealtime.FeedEntity feedEntity1 = GtfsRealtime.FeedEntity.newBuilder().setTripUpdate(tripUpdate1).setId("entity_1").build();

        GtfsRealtime.TripUpdate tripUpdate2 = GtfsRealtime.TripUpdate.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setTripId("trip_2")
                        .setDirectionId(1)
                        .setStartDate("20000101")
                        .setStartTime("06:45:00")
                        .setRouteId("3001P")
                        .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED))
                .build();

        GtfsRealtime.FeedEntity feedEntity2 = GtfsRealtime.FeedEntity.newBuilder().setTripUpdate(tripUpdate2).setId("entity_2").build();

        assertEquals(2, TripUpdatePublisher.filterTripUpdatesForGoogle(Arrays.asList(feedEntity1, feedEntity2)).size());
    }

    @Test
    public void testHasData() {
        GtfsRealtime.TripUpdate withData = GtfsRealtime.TripUpdate.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setTripId("1")
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("1")
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(0).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .build();

        assertTrue(TripUpdatePublisher.hasData(withData));

        GtfsRealtime.TripUpdate noData = GtfsRealtime.TripUpdate.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setTripId("1")
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("1")
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA)
                        .build())
                .build();

        assertFalse(TripUpdatePublisher.hasData(noData));
    }
}
