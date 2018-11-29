package fi.hsl.transitdata.gtfsbundler;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class DatasetBundler {
    private static final Logger log = LoggerFactory.getLogger(DatasetBundler.class);

    final HashMap<Long, DatasetEntry> cache = new HashMap<>();
    final int maxAgeInMs;
    final String path;

    public DatasetBundler(Config config) {
        maxAgeInMs = config.getInt("bundler.maxAgeInMinutes") * 60 * 1000;
        path = config.getString("bundler.outputPath");
    }

    public void initialize() throws Exception {
        //TODO warm up the cache by reading the latest dump?
    }

    public synchronized void bundle(List<DatasetEntry> newMessages) throws Exception {
        long startTime = System.currentTimeMillis();
        log.info("Starting GTFS Full dataset bundling");

        mergeEventsToCache(newMessages, cache);

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

        save(fullDump, path, "test.gtfsrt");

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Bundling done in {} ms", elapsed);
    }

    static void mergeEventsToCache(List<DatasetEntry> newMessages, Map<Long, DatasetEntry> cache) {
        //Messages should already come sorted by event time but let's make sure, it doesn't cost much
        Collections.sort(newMessages, Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs));
        //merge with previous entries. Only keep latest.
        newMessages.forEach(entry -> cache.put(entry.getDvjId(), entry));
    }

    static void removeOldEntries(Map<Long, DatasetEntry> cache, int maxAgeInMs, long nowUtcMs) {
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

    void save(GtfsRealtime.FeedMessage feedMessage, String path, String fileName) throws Exception {
        //TODO upload to somewhere.
        byte[] data = feedMessage.toByteArray();
        String fullPath = path.endsWith("/") ? path + fileName : path + "/" + fileName;

        Files.write(Paths.get(fullPath), data);
    }
}
