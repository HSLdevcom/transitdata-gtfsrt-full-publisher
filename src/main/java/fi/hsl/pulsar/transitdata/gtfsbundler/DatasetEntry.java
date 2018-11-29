package fi.hsl.pulsar.transitdata.gtfsbundler;

import com.google.transit.realtime.GtfsRealtime;
import org.apache.pulsar.client.api.Message;

import java.util.List;

public class DatasetEntry {
    private DatasetEntry(long id, long evetTimeMs, List<GtfsRealtime.FeedEntity> entities) {
        this.dvjId = id;
        this.entities = entities;
        this.eventTimeMs = evetTimeMs;
    }

    private long dvjId;
    private long eventTimeMs;
    /**
     * We store all entities parsed from one TripUpdate message.
     * Currently we only have one entity per message but storing the whole list makes us future-proof if this ever changes.
     */
    private List<GtfsRealtime.FeedEntity> entities;

    public static DatasetEntry newEntry(Message msg) throws Exception {
        long dvjId = Long.parseLong(msg.getKey());
        long eventTimeMs = msg.getEventTime();
        GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(msg.getData());

        return new DatasetEntry(dvjId, eventTimeMs, feedMessage.getEntityList());
    }

    public long getDvjId() {
        return dvjId;
    }

    public long getEventTimeUtcMs() {
        return eventTimeMs;
    }

    public List<GtfsRealtime.FeedEntity> getEntities() {
        return entities;
    }
}
