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

    public static DatasetEntry newEntry(Message msg) throws Exception {
        long dvjId = 0L;
        String key = msg.getKey();
        if (key != null && !key.isEmpty()) {
            dvjId = Long.parseLong(key);
        }
        long eventTimeMs = msg.getEventTime();
        GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(msg.getData());

        return new DatasetEntry(dvjId, eventTimeMs, feedMessage);
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
