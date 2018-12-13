package fi.hsl.transitdata.gtfsbundler;

import com.google.transit.realtime.GtfsRealtime;
import org.apache.pulsar.client.api.Message;

import java.util.List;
import java.util.Optional;

public class DatasetEntry {
    private DatasetEntry(long id, long evetTimeMs, GtfsRealtime.FeedMessage feedMessage) {
        this.dvjId = id;
        this.feedMessage = feedMessage;
        this.eventTimeMs = evetTimeMs;
    }

    private long dvjId;
    private long eventTimeMs;
    private GtfsRealtime.FeedMessage feedMessage;

    public static DatasetEntry newEntry(Message msg, DatasetBundler.DataType expectedType) throws Exception {
        long eventTimeMs = msg.getEventTime();
        GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(msg.getData());
        if (expectedType == DatasetBundler.DataType.ServiceAlert) {
            // No DVJ-ID for Service Alerts
            return new DatasetEntry(0L, eventTimeMs, feedMessage);
        }
        else if (expectedType == DatasetBundler.DataType.TripUpdate) {
            long dvjId = Long.parseLong(msg.getKey());
            return new DatasetEntry(dvjId, eventTimeMs, feedMessage);
        }
        else {
            throw new IllegalArgumentException("Invalid data type to expect" + expectedType);
        }
    }

    public long getDvjId() {
        return dvjId;
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
