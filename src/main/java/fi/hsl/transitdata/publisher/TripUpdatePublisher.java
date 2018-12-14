package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import org.apache.pulsar.client.api.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TripUpdatePublisher extends DatasetPublisher {
    private static final Logger log = LoggerFactory.getLogger(TripUpdatePublisher.class);

    final HashMap<Long, DatasetEntry> cache = new HashMap<>();
    final long maxAgeInMs;


    protected TripUpdatePublisher(Config config, ISink sink) {
        super(config, sink);
        maxAgeInMs = config.getDuration("bundler.tripUpdate.maxAge", TimeUnit.MILLISECONDS);
    }

    public synchronized void publish(List<DatasetEntry> newMessages) throws Exception {
        if (newMessages.isEmpty()) {
            log.warn("No new messages to publish, ignoring.");
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("Starting GTFS Full dataset publishing. Cache size: {}, new events: {}", cache.size(), newMessages.size());

        mergeEventsToCache(newMessages, cache);
        log.info("Cache size after merging: {}", cache.size());

        //filter old ones out
        log.info("Pruning cache, removing events older than {}", maxAgeInMs);
        int sizeBefore = cache.size();

        final long now = System.currentTimeMillis();
        removeOldEntries(cache, maxAgeInMs, now);
        int sizeAfter = cache.size();
        log.info("Size before pruning {} and after {}", sizeBefore, sizeAfter);

        //create GTFS RT Full dataset
        List<GtfsRealtime.FeedEntity> entities = getFeedEntities(cache);

        //Add alert if something is wrong
        if (entities.size() != cache.size()) {
            log.error("Cache size != entity-list size. Bug or is something strange happening here..?");
        }

        final long nowInSecs = now / 1000;
        GtfsRealtime.FeedMessage fullDump = createFeedMessage(entities, nowInSecs);

        sink.put(fileName, fullDump.toByteArray());

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Bundling done in {} ms", elapsed);
    }

    static void mergeEventsToCache(List<DatasetEntry> newMessages, Map<Long, DatasetEntry> cache) {
        //Messages should already come sorted by event time but let's make sure, it doesn't cost much
        Collections.sort(newMessages, Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs));
        //merge with previous entries. Only keep latest.
        newMessages.forEach(entry -> cache.put(entry.getDvjId(), entry));
    }

    static void removeOldEntries(Map<Long, DatasetEntry> cache, long maxAgeInMs, long nowUtcMs) {
        Iterator<Map.Entry<Long, DatasetEntry>> it = cache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, DatasetEntry> pair = it.next();

            long ts = pair.getValue().getEventTimeUtcMs();
            long age = nowUtcMs - ts;
            if (age > maxAgeInMs) {
                it.remove();
            }
        }
    }

    static List<GtfsRealtime.FeedEntity> getFeedEntities(Map<Long, DatasetEntry> state) {
        return state.values().stream()
                .sorted(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs).reversed()) // Sort by event time, latest first
                .map(DatasetEntry::getEntities)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    static GtfsRealtime.FeedMessage createFeedMessage(List<GtfsRealtime.FeedEntity> entities, long timestampUtcSecs) {
        GtfsRealtime.FeedHeader header = GtfsRealtime.FeedHeader.newBuilder()
                .setGtfsRealtimeVersion("2.0")
                .setIncrementality(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET)
                .setTimestamp(timestampUtcSecs)
                .build();

        return GtfsRealtime.FeedMessage.newBuilder()
                .addAllEntity(entities)
                .setHeader(header)
                .build();
    }

}
