package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

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

        assertEquals(dateTime.toEpochSecond(), TripUpdatePublisher.getExpirationTimeForCancellation(tripUpdate, ZoneId.of("UTC"), maxAge));
    }
}
