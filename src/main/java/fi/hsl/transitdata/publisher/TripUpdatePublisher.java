package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TripUpdatePublisher extends DatasetPublisher {
    private static final Logger log = LoggerFactory.getLogger(TripUpdatePublisher.class);

    final HashMap<String, DatasetEntry> cache = new HashMap<>();
    final long maxAgeInSecs;


    protected TripUpdatePublisher(Config config, ISink sink) {
        super(config, sink);
        maxAgeInSecs = config.getDuration("bundler.tripUpdate.contentMaxAge", TimeUnit.SECONDS);
    }

    public void initialize() throws Exception {
        //TODO warm up the cache by reading the latest dump?
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
        log.info("Pruning cache, removing events older than {} secs", maxAgeInSecs);
        int sizeBefore = cache.size();

        final long nowInSecs = System.currentTimeMillis() / 1000;
        removeOldEntries(cache, maxAgeInSecs, nowInSecs);
        int sizeAfter = cache.size();
        log.info("Size before pruning {} and after {}", sizeBefore, sizeAfter);

        //create GTFS RT Full dataset
        List<GtfsRealtime.FeedEntity> entities = getFeedEntities(cache);

        //Add alert if something is wrong
        if (entities.size() != cache.size()) {
            log.error("Cache size != entity-list size. Bug or is something strange happening here..?");
        }

        GtfsRealtime.FeedMessage fullDump = FeedMessageFactory.createFullFeedMessage(entities, nowInSecs);

        sink.put(fileName, fullDump.toByteArray());

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Bundling done in {} ms", elapsed);
    }

    static void mergeEventsToCache(List<DatasetEntry> newMessages, Map<String, DatasetEntry> cache) {
        //Messages should already come sorted by event time but let's make sure, it doesn't cost much
        Collections.sort(newMessages, Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs));
        //merge with previous entries. Only keep latest.
        newMessages.forEach(entry -> cache.put(entry.getId(), entry));
    }

    static void removeOldEntries(Map<String, DatasetEntry> cache, long keepAfterLastEventInSecs, long nowInSecs) {
        Iterator<Map.Entry<String, DatasetEntry>> it = cache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, DatasetEntry> pair = it.next();
            // Note! By filtering out the whole FeedMessage we assume that it only contains
            // FeedEntities for a single trip. Currently this is so, but needs to be fixed if things change.
            // Let's add a warning which should be monitored
            GtfsRealtime.FeedMessage feedMessage = pair.getValue().getFeedMessage();
            if (feedMessage.getEntityCount() != 1) {
                log.error("FeedMessage entity count != 1. count: {}", feedMessage.getEntityCount());
                for (GtfsRealtime.FeedEntity entity: feedMessage.getEntityList()) {
                    log.debug("FeedEntity Id: {}", entity.getId());
                }
            }

            long latestTimestampInFeedMessage = findLatestTimestamp(feedMessage);
            long age = nowInSecs - latestTimestampInFeedMessage;
            if (age > keepAfterLastEventInSecs) {
                log.debug("Removing because age is {} s", age);
                it.remove();
            }
        }
    }

    protected static Long findLatestTimestamp(GtfsRealtime.FeedMessage msg) {
        Optional<Long> maxTimestamp = msg.getEntityList().stream()
                .map(GtfsRealtime.FeedEntity::getTripUpdate)
                .map(tu -> {
                    //If trip update has no stop time updates, use trip start time + 2h for "latest timestamp"
                    if (tu.getStopTimeUpdateList().isEmpty()) {
                        String[] time = tu.getTrip().getStartTime().split(":");
                        ZonedDateTime tripStartTime = LocalDate.parse(tu.getTrip().getStartDate(), DateTimeFormatter.BASIC_ISO_DATE)
                                .atStartOfDay(ZoneId.of("Europe/Helsinki"))
                                .plusHours(Long.parseLong(time[0]))
                                .plusMinutes(Long.parseLong(time[1]))
                                .plusSeconds(Long.parseLong(time[2]));

                        return Optional.of(tripStartTime.plusHours(2).toEpochSecond());
                    }

                    return tu.getStopTimeUpdateList().stream().map(
                            stu -> {
                                long max = 0L;
                                if (stu.hasArrival()) {
                                    max = stu.getArrival().getTime();
                                }
                                if (stu.hasDeparture()) {
                                    long departureTime = stu.getDeparture().getTime();
                                    if (departureTime > max) {
                                        max = departureTime;
                                    }
                                }
                                return max;
                            }
                    ).max(Comparator.naturalOrder()); // Get latest timestamp of all StopTimeUpdates in this TripUpdate
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .max(Comparator.naturalOrder()); // Get latest timestamp of all TripUpdates in this FeedEntityList
         return maxTimestamp.orElse(0L);
    }

    static List<GtfsRealtime.FeedEntity> getFeedEntities(Map<String, DatasetEntry> state) {
        return state.values().stream()
                .sorted(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs).reversed()) // Sort by event time, latest first
                .map(DatasetEntry::getEntities)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }


}
