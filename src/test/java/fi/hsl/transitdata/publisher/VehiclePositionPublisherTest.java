package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.transitdata.publisher.publisher.VehiclePositionPublisher;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class VehiclePositionPublisherTest {
    @Test
    public void testVehiclePositionsAreFilteredCorrectly() {
        GtfsRealtime.VehiclePosition metroPosition = GtfsRealtime.VehiclePosition.newBuilder()
            .setTimestamp(0)
            .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                .setDirectionId(1)
                .setStartDate("20000101")
                .setStartTime("06:45:00")
                .setRouteId("31M1")
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED))
            .build();

        GtfsRealtime.VehiclePosition trainPosition = GtfsRealtime.VehiclePosition.newBuilder()
            .setTimestamp(0)
            .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                    .setDirectionId(1)
                    .setStartDate("20000101")
                    .setStartTime("06:45:00")
                    .setRouteId("3001P")
                    .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED))
            .build();

        GtfsRealtime.VehiclePosition busPosition = GtfsRealtime.VehiclePosition.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                        .setDirectionId(1)
                        .setStartDate("20000101")
                        .setStartTime("06:45:00")
                        .setRouteId("2550")
                        .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED))
                .build();

        List<GtfsRealtime.FeedEntity> entities = Stream.of(metroPosition, trainPosition, busPosition).map(pos -> GtfsRealtime.FeedEntity.newBuilder().setId(String.valueOf(pos.hashCode())).setVehicle(pos).build()).collect(Collectors.toList());

        assertEquals(3, VehiclePositionPublisher.filterVehiclePositionsForGoogle(entities, true, true).size());
        assertEquals(1, VehiclePositionPublisher.filterVehiclePositionsForGoogle(entities, true, false).size());
        assertEquals(2, VehiclePositionPublisher.filterVehiclePositionsForGoogle(entities, false, true).size());
    }
}
