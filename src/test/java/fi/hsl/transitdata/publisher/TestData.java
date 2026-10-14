package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.transitdata.publisher.sink.ISink;
import org.apache.pulsar.client.api.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Builders shared by the characterization tests.
 */
public final class TestData {
    private TestData() {
    }

    /**
     * environment.conf (with common.conf) as Main loads it, with the given overrides on top.
     */
    public static Config config(Map<String, Object> overrides) {
        return ConfigFactory.parseMap(overrides).withFallback(ConfigParser.createConfig()).resolve();
    }

    public static Config config(String... keyValues) {
        Map<String, Object> overrides = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            overrides.put(keyValues[i], keyValues[i + 1]);
        }
        return config(overrides);
    }

    @SuppressWarnings("unchecked")
    public static Message<byte[]> pulsarMessage(byte[] data, String key, long eventTimeMs,
            TransitdataProperties.ProtobufSchema schema) {
        Message<byte[]> msg = mock(Message.class);
        when(msg.getData()).thenReturn(data);
        when(msg.getKey()).thenReturn(key);
        when(msg.getEventTime()).thenReturn(eventTimeMs);
        when(msg.getProperty(TransitdataProperties.KEY_PROTOBUF_SCHEMA))
                .thenReturn(schema == null ? null : schema.toString());
        return msg;
    }

    public static DatasetEntry entry(GtfsRealtime.FeedMessage feedMessage, String key, long eventTimeMs,
            DatasetPublisher.DataType type) throws Exception {
        return DatasetEntry.newEntry(pulsarMessage(feedMessage.toByteArray(), key, eventTimeMs, null), type);
    }

    public static GtfsRealtime.FeedMessage feedMessage(GtfsRealtime.FeedEntity... entities) {
        GtfsRealtime.FeedMessage.Builder builder = GtfsRealtime.FeedMessage.newBuilder()
                .setHeader(GtfsRealtime.FeedHeader.newBuilder().setGtfsRealtimeVersion("2.0")
                        .setIncrementality(GtfsRealtime.FeedHeader.Incrementality.DIFFERENTIAL).setTimestamp(1));
        for (GtfsRealtime.FeedEntity entity : entities) {
            builder.addEntity(entity);
        }
        return builder.build();
    }

    public static GtfsRealtime.TripDescriptor trip(String tripId, String routeId, String startDate, String startTime,
            GtfsRealtime.TripDescriptor.ScheduleRelationship scheduleRelationship) {
        return GtfsRealtime.TripDescriptor.newBuilder().setTripId(tripId).setRouteId(routeId).setDirectionId(1)
                .setStartDate(startDate).setStartTime(startTime).setScheduleRelationship(scheduleRelationship).build();
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate(String stopId, long arrivalSecs,
            long departureSecs) {
        return GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder().setStopId(stopId)
                .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arrivalSecs))
                .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departureSecs))
                .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED).build();
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate(String stopId,
            GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship scheduleRelationship) {
        return GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder().setStopId(stopId)
                .setScheduleRelationship(scheduleRelationship).build();
    }

    public static GtfsRealtime.FeedEntity tripUpdateEntity(String entityId, GtfsRealtime.TripDescriptor trip,
            GtfsRealtime.TripUpdate.StopTimeUpdate... stopTimeUpdates) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = GtfsRealtime.TripUpdate.newBuilder().setTrip(trip).setTimestamp(0);
        for (GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate : stopTimeUpdates) {
            tripUpdate.addStopTimeUpdate(stopTimeUpdate);
        }
        return GtfsRealtime.FeedEntity.newBuilder().setId(entityId).setTripUpdate(tripUpdate).build();
    }

    public static GtfsRealtime.FeedEntity vehiclePositionEntity(String vehicleId, String routeId, long timestampSecs) {
        GtfsRealtime.VehiclePosition.Builder vehiclePosition = GtfsRealtime.VehiclePosition.newBuilder()
                .setTimestamp(timestampSecs)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setRouteId(routeId).setDirectionId(1));
        if (vehicleId != null) {
            vehiclePosition.setVehicle(GtfsRealtime.VehicleDescriptor.newBuilder().setId(vehicleId));
        }
        return GtfsRealtime.FeedEntity.newBuilder().setId("vp-" + vehicleId + "-" + timestampSecs)
                .setVehicle(vehiclePosition).build();
    }

    public static GtfsRealtime.FeedEntity alertEntity(String id, String headerText) {
        return GtfsRealtime.FeedEntity.newBuilder().setId(id).setAlert(GtfsRealtime.Alert.newBuilder()
                .setHeaderText(GtfsRealtime.TranslatedString.newBuilder().addTranslation(
                        GtfsRealtime.TranslatedString.Translation.newBuilder().setText(headerText).setLanguage("fi"))))
                .build();
    }

    /**
     * Sink that records every put in memory.
     */
    public static class RecordingSink implements ISink {
        public static class Put {
            public final String containerName;
            public final String fileName;
            public final byte[] data;

            Put(String containerName, String fileName, byte[] data) {
                this.containerName = containerName;
                this.fileName = fileName;
                this.data = data;
            }

            public GtfsRealtime.FeedMessage feedMessage() throws Exception {
                return GtfsRealtime.FeedMessage.parseFrom(data);
            }
        }

        public final List<Put> puts = new ArrayList<>();
        private long lastPublishTime = 42;

        @Override
        public void put(String containerName, String fileName, byte[] data) {
            puts.add(new Put(containerName, fileName, data));
            lastPublishTime = System.nanoTime();
        }

        @Override
        public long getLastPublishTime() {
            return lastPublishTime;
        }

        public Put lastPutTo(String containerName) {
            Put last = null;
            for (Put put : puts) {
                if (put.containerName.equals(containerName)) {
                    last = put;
                }
            }
            return last;
        }
    }
}
