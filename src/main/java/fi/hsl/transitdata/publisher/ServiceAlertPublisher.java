package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class ServiceAlertPublisher extends DatasetPublisher {
    private static final Logger log = LoggerFactory.getLogger(DatasetPublisher.class);

    private final long tripUpdateMaxAgeSecs;

    private Map<String, GtfsRealtime.FeedEntity> tripUpdateCache = new HashMap<>();

    private List<GtfsRealtime.FeedEntity> latestServiceAlerts;

    public ServiceAlertPublisher(Config config, ISink sink) {
        super(config, sink);

        tripUpdateMaxAgeSecs = config.getDuration("bundler.tripUpdate.contentMaxAge", TimeUnit.SECONDS);
    }

    @Override
    public void publish(List<DatasetEntry> newMessages) throws Exception {
        //Feed messages that contain textual service alerts
        List<DatasetEntry> serviceAlerts = new ArrayList<>();

        for (DatasetEntry datasetEntry : newMessages) {
            if (datasetEntry.getEntities().stream().allMatch(GtfsRealtime.FeedEntity::hasTripUpdate)) {
                datasetEntry.getEntities().stream()
                        .filter(feedEntity -> isCancellation(feedEntity.getTripUpdate()))
                        .forEach(feedEntity -> {
                            tripUpdateCache.put(feedEntity.getId(), feedEntity);
                        });
            } else if (datasetEntry.getEntities().stream().anyMatch(GtfsRealtime.FeedEntity::hasAlert)) {
                serviceAlerts.add(datasetEntry);
            } else {
                log.warn("Unexpected feed message");
            }
        }

        //We're only interested in the first item in the list.
        // Sort by event time, latest first
        serviceAlerts.stream()
                .max(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs))
                .ifPresent(datasetEntry -> latestServiceAlerts = datasetEntry.getEntities());

        long nowSecs = System.currentTimeMillis() / 1000;
        tripUpdateCache.values().removeIf(feedEntity -> {
            long age = nowSecs - findLatestTimestamp(feedEntity);
            if (age > tripUpdateMaxAgeSecs) {
                log.debug("Removing because age is {} s", age);
                return true;
            } else {
                return false;
            }
        });

        List<GtfsRealtime.FeedEntity> feedEntities = new ArrayList<>();
        feedEntities.addAll(tripUpdateCache.values());
        if (latestServiceAlerts != null) {
            feedEntities.addAll(latestServiceAlerts);
        }

        GtfsRealtime.FeedMessage feedMessage = FeedMessageFactory.createFullFeedMessage(feedEntities, nowSecs);

        log.info("Publishing a new Service Alert");
        byte[] data = feedMessage.toByteArray();
        sink.put(fileName, data);
    }

    static boolean isCancellation(GtfsRealtime.TripUpdate tripUpdate) {
        return tripUpdate.getTrip().hasScheduleRelationship() && tripUpdate.getTrip().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED ||
                tripUpdate.getStopTimeUpdateList().stream().anyMatch(stopTimeUpdate -> stopTimeUpdate.hasScheduleRelationship() && stopTimeUpdate.getScheduleRelationship() == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
    }

    static long findLatestTimestamp(GtfsRealtime.FeedEntity entity) {
        Optional<Long> maxTimestamp = entity.getTripUpdate().getStopTimeUpdateList().stream()
                .map(stu -> {
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
                }).max(Comparator.naturalOrder()); // Get latest timestamp of all StopTimeUpdates in this TripUpdate
        return maxTimestamp.orElse(0L);
    }

}
