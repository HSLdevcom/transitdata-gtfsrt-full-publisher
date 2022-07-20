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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class VehiclePositionPublisher extends DatasetPublisher {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionPublisher.class);

    private Map<String, GtfsRealtime.FeedEntity> vehiclePositionCache = new HashMap<>(1000);

    private final long maxAgeInSecs;

    private final String fullDatasetContainerName;
    private final String busTramDatasetContainerName;
    private final String trainMetroDatasetContainerName;

    public VehiclePositionPublisher(Config config, ISink sink) {
        super(config, sink);
        maxAgeInSecs = config.getDuration("bundler.vehiclePosition.contentMaxAge", TimeUnit.SECONDS);

        fullDatasetContainerName = config.getString("bundler.vehiclePosition.fullContainerName");
        busTramDatasetContainerName = config.getString("bundler.vehiclePosition.busTramContainerName");
        trainMetroDatasetContainerName = config.getString("bundler.vehiclePosition.trainMetroContainerName");
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

        List<GtfsRealtime.FeedEntity> fullDataset = new ArrayList<>(vehiclePositionCache.values());
        List<GtfsRealtime.FeedEntity> busTramDataset = filterVehiclePositionsForGoogle(fullDataset, true, false);
        List<GtfsRealtime.FeedEntity> trainMetroDataset = filterVehiclePositionsForGoogle(fullDataset, false, true);

        publishDataset(fullDatasetContainerName, fullDataset, currentTimeSecs);
        publishDataset(busTramDatasetContainerName, busTramDataset, currentTimeSecs);
        publishDataset(trainMetroDatasetContainerName, trainMetroDataset, currentTimeSecs);

        logger.info("Vehicle positions published in {}ms", System.currentTimeMillis() - startTime);
    }

    private void publishDataset(String containerName, List<GtfsRealtime.FeedEntity> feedEntities, long currentTimeSecs) throws Exception {
        GtfsRealtime.FeedMessage vehiclePositionDump = FeedMessageFactory.createFullFeedMessage(feedEntities, currentTimeSecs);

        sink.put(containerName, fileName, vehiclePositionDump.toByteArray());
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
}
