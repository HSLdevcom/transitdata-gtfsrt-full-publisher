package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import org.apache.pulsar.client.api.Message;

import java.util.List;

public class DatasetEntry {
    private DatasetEntry(long id, long eventTimeMs, GtfsRealtime.FeedMessage feedMessage) {
        this.id = id;
        this.feedMessage = feedMessage;
        this.eventTimeMs = eventTimeMs;
    }

    private long id;
    private long eventTimeMs;
    private GtfsRealtime.FeedMessage feedMessage;

    public static DatasetEntry newEntry(Message msg, DatasetPublisher.DataType expectedType) throws Exception {
        long eventTimeMs = msg.getEventTime();
        GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(msg.getData());
        if (expectedType == DatasetPublisher.DataType.ServiceAlert || expectedType == DatasetPublisher.DataType.VehiclePosition) {
            // No ID for Service Alerts or Vehicle Positions
            return new DatasetEntry(0L, eventTimeMs, feedMessage);
        }
        else if (expectedType == DatasetPublisher.DataType.TripUpdate) {
            long id = Long.parseLong(msg.getKey());
            return new DatasetEntry(id, eventTimeMs, feedMessage);
        }
        else {
            throw new IllegalArgumentException("Invalid data type to expect" + expectedType);
        }
    }

    public long getId() {
        return id;
    }

    public long getEventTimeUtcMs() {
        return eventTimeMs;
    }

    public GtfsRealtime.FeedMessage getFeedMessage() {
        return feedMessage;
    }

    public List<GtfsRealtime.FeedEntity> getEntities() {
        return feedMessage.getEntityList();
    }
}
