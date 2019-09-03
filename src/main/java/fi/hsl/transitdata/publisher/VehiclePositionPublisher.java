package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class VehiclePositionPublisher extends DatasetPublisher {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionPublisher.class);

    private Map<String, GtfsRealtime.FeedEntity> vehiclePositionCache = new HashMap<>(1000);

    private long maxAgeInSecs;

    public VehiclePositionPublisher(Config config, ISink sink) {
        super(config, sink);
        maxAgeInSecs = config.getDuration("bundler.vehiclePosition.contentMaxAge", TimeUnit.SECONDS);
    }

    @Override
    public synchronized void publish(List<DatasetEntry> newMessages) throws Exception {
        if (newMessages.isEmpty()) {
            logger.warn("No new vehicle position messages, ignoring..");
            return;
        }

        long startTime = System.currentTimeMillis();
        logger.info("Starting GTFS Full dataset publishing. Cache size: {}, new vehicle positions: {}", vehiclePositionCache.size(), newMessages.size());

        mergeVehiclePositionsToCache(newMessages, vehiclePositionCache);
        logger.info("Cache size after merging: {}", vehiclePositionCache.size());

        logger.info("Removing vehicle positions older than {} seconds from cache", maxAgeInSecs);
        int cacheSizeBefore = vehiclePositionCache.size();

        long currentTimeSecs = System.currentTimeMillis() / 1000;
        vehiclePositionCache.entrySet().removeIf(entry -> {
            GtfsRealtime.FeedEntity entity = entry.getValue();
            long vehiclePosAge = currentTimeSecs - entity.getVehicle().getTimestamp();
            if (vehiclePosAge > maxAgeInSecs) {
                logger.debug("Removing {} because it was older than {} seconds (age: {}s)", entry.getKey(), maxAgeInSecs, vehiclePosAge);
                return true;
            } else {
                return false;
            }
        });

        logger.info("Cache size before: {}, after: {}", cacheSizeBefore, vehiclePositionCache.size());

        GtfsRealtime.FeedMessage vehiclePositionDump = FeedMessageFactory.createFullFeedMessage(new ArrayList<>(vehiclePositionCache.values()), currentTimeSecs);

        sink.put(fileName, vehiclePositionDump.toByteArray());

        logger.info("Vehicle positions published in {}ms", System.currentTimeMillis() - startTime);
    }

    static void mergeVehiclePositionsToCache(List<DatasetEntry> entries, Map<String, GtfsRealtime.FeedEntity> cache) {
        entries.sort(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs));

        entries.forEach(entry -> {
            if (entry.getEntities().size() != 1) {
                logger.warn("FeedMessage had unexpected amount of entities (!= 1), entity count: {}", entry.getEntities().size());
                entry.getEntities().forEach(entity -> logger.debug("Entity id: {}", entity.getId()));
            }

            if (entry.getEntities().isEmpty()) {
                return;
            }

            GtfsRealtime.FeedEntity entity = entry.getEntities().get(0);
            cache.compute(entity.getVehicle().getVehicle().getId(), (key, prevEntity) -> {
                if (prevEntity != null && prevEntity.getVehicle().getTimestamp() > entity.getVehicle().getTimestamp()) {
                    logger.warn("Vehicle position {} had older timestamp than previously published vehicle position", entity.getVehicle().getVehicle().getId());

                    return prevEntity;
                } else {
                    return entity;
                }
            });
        });
    }
}
