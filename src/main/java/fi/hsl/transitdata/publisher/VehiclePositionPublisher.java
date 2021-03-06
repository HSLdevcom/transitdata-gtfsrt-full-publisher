package fi.hsl.transitdata.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class VehiclePositionPublisher extends DatasetPublisher {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionPublisher.class);

    private Map<String, GtfsRealtime.FeedEntity> vehiclePositionCache = new HashMap<>(1000);

    private long maxAgeInSecs;
    private ContentType type;

    public VehiclePositionPublisher(Config config, ISink sink) {
        super(config, sink);
        maxAgeInSecs = config.getDuration("bundler.vehiclePosition.contentMaxAge", TimeUnit.SECONDS);
        type = ContentType.valueOf(config.getString("bundler.vehiclePosition.contentType"));
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

        List<GtfsRealtime.FeedEntity> filteredFeedEntities = filterVehiclePositionsForGoogle(vehiclePositionCache.values(), type == ContentType.full || type == ContentType.bustram, type == ContentType.full || type == ContentType.trainmetro);

        GtfsRealtime.FeedMessage vehiclePositionDump = FeedMessageFactory.createFullFeedMessage(filteredFeedEntities, currentTimeSecs);

        sink.put(fileName, vehiclePositionDump.toByteArray());

        logger.info("Vehicle positions published in {}ms", System.currentTimeMillis() - startTime);
    }

    static List<GtfsRealtime.FeedEntity> filterVehiclePositionsForGoogle(Collection<GtfsRealtime.FeedEntity> entities, boolean busesAndTrams, boolean metrosAndTrains) {
        return entities.stream().filter(entity -> {
            GtfsRealtime.VehiclePosition vehiclePosition = entity.getVehicle();
            String routeId = vehiclePosition.getTrip().getRouteId();
            if (busesAndTrams && metrosAndTrains) {
                return true;
            }
            if (busesAndTrams && !RouteIdUtils.isMetroRoute(routeId) && !RouteIdUtils.isTrainRoute(routeId)) {
                return true;
            }
            return metrosAndTrains && (RouteIdUtils.isTrainRoute(routeId) || RouteIdUtils.isMetroRoute(routeId));
        }).collect(Collectors.toList());
    }

    static void mergeVehiclePositionsToCache(List<DatasetEntry> entries, Map<String, GtfsRealtime.FeedEntity> cache) {
        entries.sort(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs));

        Set<String> vehiclesThatHadOlderTimestamp = new HashSet<>();

        entries.forEach(entry -> {
            if (entry.getEntities().size() != 1) {
                logger.warn("FeedMessage had unexpected amount of entities (!= 1), entity count: {}", entry.getEntities().size());
                entry.getEntities().forEach(entity -> logger.debug("Entity id: {}", entity.getId()));
            }

            if (entry.getEntities().isEmpty()) {
                return;
            }

            GtfsRealtime.FeedEntity entity = entry.getEntities().get(0);
            cache.compute(entity.getVehicle().getVehicle().getId(), (vehicleId, prevEntity) -> {
                if (prevEntity != null && prevEntity.getVehicle().getTimestamp() > entity.getVehicle().getTimestamp()) {
                    vehiclesThatHadOlderTimestamp.add(vehicleId);
                    return prevEntity;
                } else {
                    return entity;
                }
            });
        });

        if (!vehiclesThatHadOlderTimestamp.isEmpty()) {
            logger.warn("Vehicles [ {} ] had timestamp older than previously published vehicle position", String.join(", ", vehiclesThatHadOlderTimestamp));
        }
    }

    public enum ContentType {
        full,
        bustram,
        trainmetro
    }
}
