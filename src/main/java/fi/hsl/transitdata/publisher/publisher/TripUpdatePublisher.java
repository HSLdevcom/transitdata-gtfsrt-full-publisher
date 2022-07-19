package fi.hsl.transitdata.publisher.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.transitdata.publisher.DatasetEntry;
import fi.hsl.transitdata.publisher.DatasetPublisher;
import fi.hsl.transitdata.publisher.sink.ISink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
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

    private final long maxAgeAfterStartSecs;
    private final ZoneId timezone;

    private final ContentType contentType;

    protected TripUpdatePublisher(Config config, ISink sink) {
        super(config, sink);
        maxAgeInSecs = config.getDuration("bundler.tripUpdate.contentMaxAge", TimeUnit.SECONDS);

        maxAgeAfterStartSecs = config.getDuration("bundler.tripUpdate.maxAgeAfterStart", TimeUnit.SECONDS);
        timezone = ZoneId.of(config.getString("bundler.tripUpdate.timezone"));

        contentType = ContentType.valueOf(config.getString("bundler.tripUpdate.contentType"));
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

        //Update cancellation entity timestamps so that Google does not discard them as too old
        entities = entities.stream().map(entity -> updateCancellationTimestamp(entity, nowInSecs)).collect(Collectors.toList());

        if (contentType == ContentType.google) {
            //Filter trip updates to publish only trip updates that Google wants
            entities = filterTripUpdatesForGoogle(entities);
        }

        GtfsRealtime.FeedMessage fullDump = FeedMessageFactory.createFullFeedMessage(entities, nowInSecs);

        sink.put(fileName, fullDump.toByteArray());

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Bundling done in {} ms", elapsed);
    }

    static void mergeEventsToCache(List<DatasetEntry> newMessages, Map<String, DatasetEntry> cache) {
        //Messages should already come sorted by event time but let's make sure, it doesn't cost much
        newMessages.sort(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs));
        //merge with previous entries. Only keep latest.
        newMessages.forEach(entry -> cache.put(entry.getId(), entry));
    }

    static List<GtfsRealtime.FeedEntity> filterTripUpdatesForGoogle(List<GtfsRealtime.FeedEntity> feedEntities) {
        return feedEntities.stream().filter(feedEntity -> {
            return feedEntity.hasTripUpdate() &&
                    feedEntity.getTripUpdate().hasTrip() &&
                    feedEntity.getTripUpdate().getTrip().hasScheduleRelationship() &&
                    feedEntity.getTripUpdate().getTrip().hasRouteId() &&
                    (feedEntity.getTripUpdate().getTrip().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED ||
                            RouteIdUtils.isMetroRoute(feedEntity.getTripUpdate().getTrip().getRouteId()) ||
                            RouteIdUtils.isTrainRoute(feedEntity.getTripUpdate().getTrip().getRouteId()));
        }).collect(Collectors.toList());
    }

    private void removeOldEntries(Map<String, DatasetEntry> cache, long keepAfterLastEventInSecs, long nowInSecs) {
        cache.values().removeIf(datasetEntry -> {
            // Note! By filtering out the whole FeedMessage we assume that it only contains
            // FeedEntities for a single trip. Currently this is so, but needs to be fixed if things change.
            // Let's add a warning which should be monitored
            GtfsRealtime.FeedMessage feedMessage = datasetEntry.getFeedMessage();
            if (feedMessage.getEntityCount() != 1) {
                log.error("FeedMessage entity count != 1. count: {}", feedMessage.getEntityCount());
                for (GtfsRealtime.FeedEntity entity: feedMessage.getEntityList()) {
                    log.debug("FeedEntity Id: {}", entity.getId());
                }
            }

            long expirationTime = feedMessage.getEntityList().stream()
                    .filter(GtfsRealtime.FeedEntity::hasTripUpdate)
                    .map(GtfsRealtime.FeedEntity::getTripUpdate)
                    .filter(TripUpdatePublisher::hasData) //Filter trip updates that have only stop times updates with NO_DATA
                    .map(tripUpdate -> {
                        if (shouldUseExpirationTime(tripUpdate)) {
                            //If trip has no stop time updates with timestamps, use trip start time + certain duration for expiration time when the trip update will be removed from the feed
                            return getExpirationTime(tripUpdate, timezone, maxAgeAfterStartSecs);
                        } else {
                            return getLatestTimestampFromStopTimeUpdates(tripUpdate);
                        }
                    })
                    .max(Comparator.naturalOrder())
                    .orElse(0L);

            long age = nowInSecs - expirationTime;
            if (age > keepAfterLastEventInSecs) {
                log.debug("Removing because age is {} s", age);
                return true;
            } else {
                return false;
            }
        });
    }

    static boolean hasData(GtfsRealtime.TripUpdate tu) {
        //Canceled trips should not be filtered out even if they have no stop time updates
        if (tu.getTrip().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
            return true;
        }

        return tu.getStopTimeUpdateList().stream()
                .anyMatch(stopTimeUpdate -> !stopTimeUpdate.hasScheduleRelationship() || //No schedule relationship -> defaults to SCHEDULED
                        stopTimeUpdate.getScheduleRelationship() != GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA);
    }

    static long getExpirationTime(GtfsRealtime.TripUpdate tu, ZoneId timezone, long maxAgeAfterStartSecs) {
        final LocalDate date = LocalDate.parse(tu.getTrip().getStartDate(), DateTimeFormatter.BASIC_ISO_DATE);

        final String[] timeParts = tu.getTrip().getStartTime().split(":");
        final int hours = Integer.parseInt(timeParts[0]);
        final int minutes = Integer.parseInt(timeParts[1]);
        final int seconds = Integer.parseInt(timeParts[2]);

        final LocalDateTime localDateTime = hours >= 24 ?
                date.plusDays(1).atTime(hours - 24, minutes, seconds) :
                date.atTime(hours, minutes, seconds);

        final ZonedDateTime tripStartTime = localDateTime.atZone(timezone);
        return tripStartTime.plusSeconds(maxAgeAfterStartSecs).toEpochSecond();
    }
      
    static boolean shouldUseExpirationTime(GtfsRealtime.TripUpdate tripUpdate) {
        //Trip is cancelled and has no stop time updates
        if (tripUpdate.getStopTimeUpdateCount() == 0 &&
                tripUpdate.getTrip().hasScheduleRelationship() &&
                tripUpdate.getTrip().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
            return true;
        }

        //Trip update has only stop cancellations
        return tripUpdate.getStopTimeUpdateList()
                .stream()
                .map(GtfsRealtime.TripUpdate.StopTimeUpdate::getScheduleRelationship)
                .allMatch(scheduleRelationship -> {
                    return scheduleRelationship == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED ||
                            scheduleRelationship == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA;
                });
    }

    static long getLatestTimestampFromStopTimeUpdates(GtfsRealtime.TripUpdate tu) {
        return tu.getStopTimeUpdateList().stream()
                .map(stopTimeUpdate -> Math.max(stopTimeUpdate.hasArrival() ? stopTimeUpdate.getArrival().getTime() : 0, stopTimeUpdate.hasDeparture() ? stopTimeUpdate.getDeparture().getTime() : 0))
                .max(Comparator.naturalOrder())
                .orElse(0L);
    }

    static List<GtfsRealtime.FeedEntity> getFeedEntities(Map<String, DatasetEntry> state) {
        return state.values().stream()
                .sorted(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs).reversed()) // Sort by event time, latest first
                .map(DatasetEntry::getEntities)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    static GtfsRealtime.FeedEntity updateCancellationTimestamp(GtfsRealtime.FeedEntity feedEntity, long timeInSecs) {
        if (feedEntity.hasTripUpdate() &&
            feedEntity.getTripUpdate().hasTrip() &&
            feedEntity.getTripUpdate().getTrip().hasScheduleRelationship() &&
            feedEntity.getTripUpdate().getTrip().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
            return feedEntity.toBuilder().setTripUpdate(feedEntity.getTripUpdate().toBuilder().setTimestamp(timeInSecs)).build();
        } else {
            return feedEntity;
        }
    }

    public enum ContentType {
        full, //Publish all trip updates
        google //Publish all trip updates for metros and trains and only cancellations for other transport modes
    }
}
