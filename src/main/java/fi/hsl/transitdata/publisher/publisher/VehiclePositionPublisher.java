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

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class VehiclePositionPublisher extends DatasetPublisher {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionPublisher.class);

    private Map<String, GtfsRealtime.FeedEntity> vehiclePositionCache = new HashMap<>(1000);

    private final Duration maxAge;

    private final String fullDatasetContainerName;
    private final String busTramDatasetContainerName;
    private final String trainMetroDatasetContainerName;

    public VehiclePositionPublisher(Config config, ISink sink) {
        super(config, sink);
        maxAge = config.getDuration("bundler.vehiclePosition.contentMaxAge");

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
        long startTime = System.nanoTime();
        logger.info("Starting GTFS Full dataset publishing. Cache size: {}, new vehicle positions: {}", vehiclePositionCache.size(), newMessages.size());

        //Sort new messages by timestamp to make sure that the latest message gets published
        newMessages.sort(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs));

        mergeVehiclePositionsToCache(newMessages, vehiclePositionCache);
        logger.info("Cache size after merging: {}", vehiclePositionCache.size());

        final Instant currentTime = Instant.now();

        Optional<Instant> vpMaxTimestamp = vehiclePositionCache.values().stream().max(Comparator.comparing(feedEntity -> feedEntity.getVehicle().getTimestamp())).map(feedEntity -> Instant.ofEpochSecond(feedEntity.getVehicle().getTimestamp()));
        Optional<Instant> vpMinTimestamp = vehiclePositionCache.values().stream().min(Comparator.comparing(feedEntity -> feedEntity.getVehicle().getTimestamp())).map(feedEntity -> Instant.ofEpochSecond(feedEntity.getVehicle().getTimestamp()));
        logger.info("Current time: {}, min vehicle timestamp in cache: {}, max vehicle timestamp in cache: {}", currentTime, vpMinTimestamp.orElse(null), vpMaxTimestamp.orElse(null));

        pruneVehiclePositionCache(currentTime);

        if (vehiclePositionCache.size() == 0) {
            logger.warn("Publishing empty dataset");
        }

        List<GtfsRealtime.FeedEntity> fullDataset = new ArrayList<>(vehiclePositionCache.values());
        List<GtfsRealtime.FeedEntity> busTramDataset = filterVehiclePositionsForGoogle(fullDataset, true, false);
        List<GtfsRealtime.FeedEntity> trainMetroDataset = filterVehiclePositionsForGoogle(fullDataset, false, true);

        publishDataset(fullDatasetContainerName, fullDataset, currentTime);
        publishDataset(busTramDatasetContainerName, busTramDataset, currentTime);
        publishDataset(trainMetroDatasetContainerName, trainMetroDataset, currentTime);

        logger.info("Vehicle positions published in {}ms", Duration.ofNanos(System.nanoTime() - startTime).toMillis());
    }

    private void pruneVehiclePositionCache(Instant currentTime) {
        logger.info("Removing vehicle positions older than {} seconds from cache", maxAge.getSeconds());
        final int cacheSizeBefore = vehiclePositionCache.size();

        final long currentTimeSecs = currentTime.getEpochSecond();
        vehiclePositionCache.entrySet().removeIf(entry -> {
            final GtfsRealtime.FeedEntity entity = entry.getValue();
            final Duration vehiclePosAge = Duration.ofSeconds(currentTimeSecs - entity.getVehicle().getTimestamp());

            if (vehiclePosAge.compareTo(maxAge) > 0) {
                logger.debug("Removing {} because it was older than {} seconds (age: {}s)", entry.getKey(), maxAge.getSeconds(), vehiclePosAge);
                return true;
            } else {
                return false;
            }
        });

        logger.info("Cache size before removing old vehicle positions: {}, after: {}", cacheSizeBefore, vehiclePositionCache.size());
    }

    private void publishDataset(String containerName, List<GtfsRealtime.FeedEntity> feedEntities, Instant currentTime) throws Exception {
        GtfsRealtime.FeedMessage vehiclePositionDump = FeedMessageFactory.createFullFeedMessage(feedEntities, currentTime.getEpochSecond());

        sink.put(containerName, fileName, vehiclePositionDump.toByteArray());
    }

    public static List<GtfsRealtime.FeedEntity> filterVehiclePositionsForGoogle(Collection<GtfsRealtime.FeedEntity> entities, boolean busesAndTrams, boolean metrosAndTrains) {
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
