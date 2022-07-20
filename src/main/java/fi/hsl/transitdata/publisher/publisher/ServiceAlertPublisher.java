package fi.hsl.transitdata.publisher.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.transitdata.publisher.DatasetEntry;
import fi.hsl.transitdata.publisher.DatasetPublisher;
import fi.hsl.transitdata.publisher.sink.ISink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class ServiceAlertPublisher extends DatasetPublisher {
    private static final Logger log = LoggerFactory.getLogger(DatasetPublisher.class);

    private final String containerName;

    public ServiceAlertPublisher(Config config, ISink sink) {
        super(config, sink);

        containerName = config.getString("bundler.serviceAlert.containerName");
    }

    @Override
    public void publish(List<DatasetEntry> newMessages) throws Exception {
        //We're only interested in the first item in the list.
        // Sort by event time, latest first
        Optional<DatasetEntry> latest = newMessages.stream().max(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs));
        if (latest.isPresent()) {
            GtfsRealtime.FeedMessage msg = latest.get().getFeedMessage();

            log.info("Publishing a new Service Alert");
            byte[] data = msg.toByteArray();
            sink.put(containerName, fileName, data);
        }
    }


}
